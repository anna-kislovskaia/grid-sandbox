package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterReportRecord;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import lombok.extern.log4j.Log4j2;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.grid.sandbox.core.model.UpdateEvent.getRecordVersion;

@Log4j2
public class BlotterFeedService<K, V extends BlotterReportRecord<K>> {
    private final BehaviorSubject<ConcurrentMap<K, V>> snapshotPublisher = BehaviorSubject.create();
    private final Flowable<ConcurrentMap<K, V>> snapshotFlowable = snapshotPublisher.toFlowable(BackpressureStrategy.LATEST);
    private final Subject<Collection<V>> updatePublisher = PublishSubject.create();
    private final Flowable<Collection<V>> updateFlowable = updatePublisher.toFlowable(BackpressureStrategy.MISSING);
    private final AtomicReference<UpdateEvent<K, V>> lastSnapshotEvent = new AtomicReference<>();
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
            lastSnapshotEvent.set(null);
            UpdateEvent<K, V> snapshotEvent = createSnapshotEvent(snapshot);
            return updateFlowable
                    .onBackpressureBuffer(updateEventBufferSize)
                    .map(updates -> {
                        try {
                            snapshotLock.writeLock().lock();
                            lastSnapshotEvent.set(null);
                            UpdateEvent<K, V> event  = handleUpdates(updates, snapshot);
                            log.info("Update processed: {} -> {}. Total {}", updates.size(), event.getUpdates().size(), snapshot.size());
                            return event;
                        } finally {
                            snapshotLock.writeLock().unlock();
                        }
                    })
                    .startWithItem(snapshotEvent)
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
            if (getRecordVersion(old) <= value.getRecordVersion()) {
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

    public UpdateEvent<K, V> getSnapshot() {
        Map<K, V> snapshot = snapshotPublisher.getValue();
        if (snapshot == null) {
            return new UpdateEvent<>(Collections.emptyList(), UpdateEvent.Type.SNAPSHOT);
        } else {
            return createSnapshotEvent(snapshot);
        }
    }

    public Flowable<UpdateEvent<K, V>> getFeed(String feedId, Scheduler scheduler) {
        log.info("{}: Feed requested", feedId);
        ConnectableFlowable<UpdateEvent<K, V>> eventFeed = updateEventFlowable
                .doOnSubscribe(s -> log.info("{}: Event feed subscribed", feedId))
                .replay(updateEventBufferSize);
        Disposable eventFeedConnection = eventFeed.connect();
        log.info("{}: Calculate initial snapshot", feedId);
        UpdateEvent<K, V> initialSnapshot = getSnapshot();
        return eventFeed.startWithItem(initialSnapshot)
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .doOnError(log::error)
                .doOnCancel(() -> {
                    if (!eventFeedConnection.isDisposed()) {
                        log.info("{}: Disconnect event feed", feedId);
                        eventFeedConnection.dispose();
                    }
                })
                .filter(event -> !event.isEmpty());
    }

    private  UpdateEvent<K, V> createSnapshotEvent(Map<K, V> snapshot) {
        log.info("Snapshot event requested");
        UpdateEvent<K, V> lastSnapshot = lastSnapshotEvent.get();
        if (lastSnapshot != null) {
            log.info("Snapshot event {}", lastSnapshot.getUpdates().size());
            return lastSnapshot;
        }
        List<UpdateEventEntry<K, V>> eventSnapshot;
        try {
            snapshotLock.readLock().lock();
            eventSnapshot = snapshot.values().stream()
                    .map(UpdateEventEntry::addedValue)
                    .collect(Collectors.toList());
            lastSnapshot = new UpdateEvent<>(eventSnapshot, UpdateEvent.Type.SNAPSHOT);
            lastSnapshotEvent.compareAndSet(null, lastSnapshot);
            log.info("Snapshot event generated {}", eventSnapshot.size());
        } finally {
            snapshotLock.readLock().unlock();
        }
        return lastSnapshot;
    }
}

