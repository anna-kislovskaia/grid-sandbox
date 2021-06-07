package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterReportRecord;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import lombok.extern.log4j.Log4j2;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Log4j2
public class BlotterFeedService<K, V extends BlotterReportRecord<K>> {
    private final Subject<ConcurrentMap<K, V>> snapshotPublisher = BehaviorSubject.create();
    private final Flowable<ConcurrentMap<K, V>> snapshotFlowable = snapshotPublisher.toFlowable(BackpressureStrategy.LATEST);
    private final Flowable<UpdateEvent<K, V>> snapshotEventFlowable = snapshotFlowable.map(BlotterFeedService::createSnapshotEvent);
    private final Subject<Collection<V>> updatePublisher = PublishSubject.create();
    private final Flowable<Collection<V>> updateFlowable = updatePublisher.toFlowable(BackpressureStrategy.MISSING);
    private final ReadWriteLock snapshotLock = new ReentrantReadWriteLock();

    private int updateEventBufferSize = 32000;

    private Flowable<UpdateEvent<K, V>> updateEventFlowable;


    public void reset(Collection<V> values) {
        log.info("Loading snapshot...");
        ConcurrentMap<K, V> snapshot = values.stream().collect(Collectors.toConcurrentMap(BlotterReportRecord::getRecordKey, value -> value));
        snapshotPublisher.onNext(snapshot);
        log.info("Snapshot loaded {}", snapshot.size());
    }

    public void update(Collection<V> values) {
        if (!values.isEmpty()) {
            log.info("Schedule updates {}", values.size());
            updatePublisher.onNext(values);
        }
    }

    @PostConstruct
    public void init() {
        updateEventFlowable = snapshotFlowable.switchMap(snapshot -> {
            log.info("Snapshot updated {}", snapshot.size());
            return updateFlowable
                    .onBackpressureBuffer(updateEventBufferSize)
                    .map(updates -> {
                        try {
                            snapshotLock.writeLock().lock();
                            UpdateEvent<K, V> event  = handleUpdates(updates, snapshot);
                            log.info("Update processed: {} -> {}. Total {}", updates.size(), event.getUpdates().size(), snapshot.size());
                            return event;
                        } finally {
                            snapshotLock.writeLock().unlock();
                        }
                    })
                    .startWithItem(createSnapshotEvent(snapshot))
                    .filter(event -> !event.isEmpty());
                }
        ).share();
        updateEventFlowable.subscribe();
    }

    private UpdateEvent<K, V> handleUpdates(Collection<V> updates, ConcurrentMap<K, V> snapshot) {
        log.info("Process update event: {}", updates.size());
        Map<K, V> previous = new HashMap<>();
        Map<K, V> current = new HashMap<>();
        for (V value : updates) {
            K key = value.getRecordKey();
            V old = current.getOrDefault(key, snapshot.get(key));
            if (getRecordVersion(old) < value.getRecordVersion()) {
                log.info("Apply record update {}: {} -> {}", key, getRecordVersion(old), value.getRecordVersion());
                current.put(key, value);
                if (!previous.containsKey(key)) {
                    previous.put(key, old);
                }
           } else {
                log.info("Stale record {}", value);
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

    public Flowable<UpdateEvent<K, V>> getFeed(Scheduler scheduler) {
        log.info("Feed requested");
        List<UpdateEvent<K, V>> processed = new CopyOnWriteArrayList<>();
        Flowable<UpdateEvent<K, V>> initialSnapshotFeed = snapshotFlowable.take(1)
                .map(snapshot -> {
                    try {
                        snapshotLock.readLock().lock();
                        return createSnapshotEvent(snapshot);
                    } finally {
                        snapshotLock.readLock().unlock();
                    }
                })
                .subscribeOn(scheduler)
                .observeOn(scheduler);
        Flowable<UpdateEvent<K, V>> eventFeed = updateEventFlowable
                .onBackpressureBuffer(updateEventBufferSize)
                .observeOn(scheduler);
        AtomicBoolean initialSnapshotReceived = new AtomicBoolean();
        return Flowable.merge(eventFeed, initialSnapshotFeed)
                .map(event -> {
                    if (!initialSnapshotReceived.get()) {
                        if (event.isSnapshot()) {
                            UpdateEvent<K, V> snapshotEvent = mergeBufferedSnapshotUpdates(event, processed);
                            log.info("Initial snapshot received {}", event.toShortString());
                            initialSnapshotReceived.set(true);
                            return snapshotEvent;
                        } else {
                            log.info("Buffer update event {}", event);
                            processed.add(event);
                            return UpdateEvent.<K, V>empty();
                        }
                    }
                    return event;
                })
                .doOnError(log::error)
                .filter(event -> !event.isEmpty());
    }

    static  <K, V extends BlotterReportRecord<K>>  UpdateEvent<K, V> createSnapshotEvent(Map<K, V> snapshot) {
        log.info("Snapshot event requested");
        List<UpdateEventEntry<K, V>> eventSnapshot = snapshot.values().stream()
                .map(UpdateEventEntry::addedValue)
                .collect(Collectors.toList());
        log.info("Snapshot event created {}", eventSnapshot.size());
        return new UpdateEvent<>(eventSnapshot, UpdateEvent.Type.SNAPSHOT);
    }

    public static  <K, V extends BlotterReportRecord<K>> UpdateEvent<K, V> mergeBufferedSnapshotUpdates(
            UpdateEvent<K, V> original,
            List<UpdateEvent<K, V>> bufferedUpdates)
    {
        if (bufferedUpdates.isEmpty()) {
            return original;
        }
        log.info("Process {} buffered updates to initial snapshot", bufferedUpdates.size());
        Map<K, UpdateEventEntry<K, V>> snapshot = original.getUpdates().stream()
                .collect(Collectors.toMap(UpdateEventEntry::getRecordKey, entry -> entry));
        bufferedUpdates.stream()
                .flatMap(updateEvent -> updateEvent.getUpdates().stream())
                .forEach(entry -> {
                    UpdateEventEntry<K, V> current = snapshot.get(entry.getRecordKey());
                    long currentVersion = UpdateEventEntry.getVersion(current);
                    if (currentVersion < entry.getVersion()) {
                        log.info("Update snapshot from buffer id={}: {} -> {}", entry.getRecordKey(), currentVersion, entry.getVersion());
                        snapshot.put(entry.getRecordKey(), UpdateEventEntry.addedValue(entry.getValue()));
                    }
                });
        log.info("Buffered updates processed");
        return new UpdateEvent<>(snapshot.values(), UpdateEvent.Type.SNAPSHOT);
    }

    private static long getRecordVersion(BlotterReportRecord<?> record) {
        return record == null ? 0 : record.getRecordVersion();
    }
}

