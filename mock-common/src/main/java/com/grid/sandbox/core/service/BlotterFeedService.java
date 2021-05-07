package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterReportRecord;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import lombok.extern.log4j.Log4j2;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Log4j2
public class BlotterFeedService<K, V extends BlotterReportRecord<K>> {
    private final Subject<ConcurrentMap<K, V>> snapshotPublisher = BehaviorSubject.create();
    private final Flowable<ConcurrentMap<K, V>> snapshotFlowable = snapshotPublisher.toFlowable(BackpressureStrategy.LATEST);
    private final Flowable<UpdateEvent<K, V>> snapshotEventFlowable = snapshotFlowable.map(snapshot -> {
        List<UpdateEventEntry<K, V>> eventSnapshot = snapshot.values().stream()
                .map(UpdateEventEntry::addedValue)
                .collect(Collectors.toList());
        return new UpdateEvent<>(eventSnapshot, UpdateEvent.Type.SNAPSHOT);
    });
    private final Subject<Collection<V>> updatePublisher = PublishSubject.create();
    private final Flowable<Collection<V>> updateFlowable = updatePublisher.toFlowable(BackpressureStrategy.MISSING);

    private int updateEventBufferSize = 1024;

    private Flowable<UpdateEvent<K, V>> updateEventFlowable;


    public void reset(Collection<V> values) {
        log.info("Loading snapshot...");
        ConcurrentMap<K, V> snapshot = values.stream().collect(Collectors.toConcurrentMap(BlotterReportRecord::getRecordKey, value -> value));
        snapshotPublisher.onNext(snapshot);
        log.info("Snapshot loaded {}", snapshot.size());
    }

    public void update(Collection<V> values) {
        log.info("Updates received {}", values.size());
        if (!values.isEmpty()) {
            updatePublisher.onNext(values);
        }
    }

    @PostConstruct
    public void init() {
        updateEventFlowable = snapshotFlowable.switchMap(snapshot ->
                updateFlowable
                   .onBackpressureBuffer(updateEventBufferSize)
                   .map(updates -> {
                        UpdateEvent<K, V> event;
                        synchronized (snapshot) {
                            event = handleUpdates(updates, snapshot);
                        }
                        log.info("Update processed: {} -> {}. Total {}", updates.size(), event.getUpdates().size(), snapshot.size());
                        return event;
                   })
                   .filter(event -> !event.isEmpty())
        ).share();
        updateEventFlowable.subscribe();
    }

    private UpdateEvent<K, V> handleUpdates(Collection<V> updates, ConcurrentMap<K, V> snapshot) {
        log.info("Apply update event: {}", updates.size());
        Map<K, V> previous = new HashMap<>();
        Map<K, V> current = new HashMap<>();
        for (V value : updates) {
            K key = value.getRecordKey();
            current.putIfAbsent(key, snapshot.get(key));
            V old = current.get(key);
            long oldVersion = old == null ? 0 : old.getRecordVersion();
            if (oldVersion < value.getRecordVersion()) {
                log.info("Apply record update {}: {} -> {}", key, oldVersion, value.getRecordVersion());
                current.put(key, value);
                if (!previous.containsKey(key)) {
                    previous.put(key, old);
                }
           } else {
                log.debug("Stale record {}", value);
            }
        }
        List<UpdateEventEntry<K, V>> recordUpdates = current.keySet().stream()
                .map(key -> new UpdateEventEntry<>(current.get(key), previous.get(key)))
                .collect(Collectors.toList());
        snapshot.putAll(current);
        return new UpdateEvent<>(recordUpdates, UpdateEvent.Type.INCREMENTAL);
    }

    public Flowable<UpdateEvent<K, V>> getSnapshotFeed() {
        return snapshotEventFlowable;
    }


    public Flowable<UpdateEvent<K, V>> getFeed() {
        return Flowable.merge(snapshotEventFlowable, updateEventFlowable);
    }
}
