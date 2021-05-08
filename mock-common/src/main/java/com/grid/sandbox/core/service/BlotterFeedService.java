package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterReportRecord;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import lombok.extern.log4j.Log4j2;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@Log4j2
public class BlotterFeedService<K, V extends BlotterReportRecord<K>> {
    private final Subject<ConcurrentMap<K, V>> snapshotPublisher = BehaviorSubject.create();
    private final Flowable<ConcurrentMap<K, V>> snapshotFlowable = snapshotPublisher.toFlowable(BackpressureStrategy.LATEST);
    private final Flowable<UpdateEvent<K, V>> snapshotEventFlowable = snapshotFlowable.map(this::createSnapshotEvent);
    private final Subject<Collection<V>> updatePublisher = PublishSubject.create();
    private final Flowable<Collection<V>> updateFlowable = updatePublisher.toFlowable(BackpressureStrategy.MISSING);

    private int updateEventBufferSize = 4096;

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
                        UpdateEvent<K, V> event;
                        synchronized (snapshot) {
                            event = handleUpdates(updates, snapshot);
                        }
                        log.info("Update processed: {} -> {}. Total {}", updates.size(), event.getUpdates().size(), snapshot.size());
                        return event;
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

    public Flowable<UpdateEvent<K, V>> getFeed(Scheduler scheduler) {
        log.info("Feed requested");
        BlockingQueue<UpdateEvent<K, V>> bufferedEvents = new ArrayBlockingQueue<>(updateEventBufferSize);
        Disposable bufferedSubscription = updateEventFlowable
                .subscribeOn(scheduler)
                .subscribe(event -> {
                    log.info("Buffer update event {}", event);
                    bufferedEvents.add(event);
                });
        List<UpdateEvent<K, V>> processed = new CopyOnWriteArrayList<>();
        return snapshotEventFlowable
                .subscribeOn(scheduler)
                .take(1)
                .switchMap(snapshot -> updateEventFlowable.subscribeOn(scheduler).startWithItem(snapshot))
                .map(event -> {
                    if (!bufferedSubscription.isDisposed()) {
                        List<UpdateEvent<K, V>> missedEvents = new ArrayList<>(bufferedEvents.size());
                        if (event.isSnapshot() && processed.isEmpty()) {
                            bufferedEvents.drainTo(missedEvents);
                            UpdateEvent<K, V> snapshot = mergeBufferedSnapshotUpdates(event, missedEvents);
                            processed.add(snapshot);
                            processed.addAll(missedEvents);
                            return snapshot;
                        } else {
                            bufferedSubscription.dispose();
                            log.info("Dispose buffer subscription");
                            bufferedEvents.drainTo(missedEvents);
                            Map<K, UpdateEventEntry<K, V>> reportedRecords = processed.stream()
                                    .flatMap(reported -> reported.getUpdates().stream())
                                    .collect(Collectors.toMap(UpdateEventEntry::getRecordKey, entry -> entry, greaterVersionMerger()));
                            processed.addAll(missedEvents);
                            int index = processed.indexOf(event);
                            int size = processed.size();
                            log.info("Processed event index {} of size {}", index, size);
                            if (index < 0) {
                                missedEvents.add(event);
                            } else {
                                // cleanup processed event list
                                List<UpdateEvent<K, V>> copy = new ArrayList<>(processed);
                                processed.clear();
                                if (index + 1 < size) {
                                    processed.addAll(copy.subList(index + 1, size));
                                }
                            }
                            return mergeBufferedUpdateEvents(reportedRecords, missedEvents);
                        }
                    }
                    if (!processed.isEmpty() && processed.remove(event)) {
                        // skip processed event
                        return new UpdateEvent<K, V>(Collections.emptyList(), UpdateEvent.Type.INCREMENTAL);
                    }
                    return event;
                })
                .filter(event -> !event.isEmpty());
    }

    private static <T, E extends BlotterReportRecord<T>> BinaryOperator<UpdateEventEntry<T, E>> greaterVersionMerger() {
        return (value1, value2) -> value1.getVersion() > value2.getVersion() ? value1 : value2;
    }

    private UpdateEvent<K, V> createSnapshotEvent(Map<K, V> snapshot) {
        log.info("Snapshot event requested");
        List<UpdateEventEntry<K, V>> eventSnapshot = snapshot.values().stream()
                .map(UpdateEventEntry::addedValue)
                .collect(Collectors.toList());
        log.info("Snapshot event created {}", eventSnapshot.size());
        return new UpdateEvent<>(eventSnapshot, UpdateEvent.Type.SNAPSHOT);
    }

    private UpdateEvent<K, V> mergeBufferedSnapshotUpdates(UpdateEvent<K, V> original, List<UpdateEvent<K, V>> bufferedUpdates) {
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
                    long currentVersion = current == null ? 0 : current.getVersion();
                    if (currentVersion < entry.getVersion()) {
                        log.info("Update snapshot from buffer {}: {} -> {}", entry.getRecordKey(), currentVersion, entry.getVersion());
                        snapshot.put(entry.getRecordKey(), UpdateEventEntry.addedValue(entry.getValue()));
                    }
                });
        log.info("Buffered updates processed");
        return new UpdateEvent<>(snapshot.values(), UpdateEvent.Type.SNAPSHOT);
    }

    private UpdateEvent<K, V> mergeBufferedUpdateEvents(Map<K, UpdateEventEntry<K, V>> reportedRecords, List<UpdateEvent<K, V>> bufferedUpdates) {
        if (bufferedUpdates.size() == 1) {
            return bufferedUpdates.get(0);
        }
        log.info("Merge {} buffered update events", bufferedUpdates.size());
        Map<K, UpdateEventEntry<K, V>> entries = new HashMap<>();
        bufferedUpdates.stream()
                .flatMap(event -> event.getUpdates().stream())
                .forEach(entry -> {
                    K key = entry.getRecordKey();
                    UpdateEventEntry<K, V> current = entries.get(key);
                    if (current == null) {
                        UpdateEventEntry<K, V> reported = reportedRecords.get(key);
                        if (reported == null || reported.getVersion() < entry.getVersion()) {
                            V oldValue = reported != null ? reported.getValue() : entry.getOldValue();
                            long oldVersion = oldValue != null ? oldValue.getRecordVersion() : 0;
                            log.info("Update from buffer {}: {} -> {}", entry.getRecordKey(), oldVersion, entry.getVersion());
                            entries.put(key, new UpdateEventEntry<>(entry.getValue(), oldValue));
                        }
                    } else {
                        if (entry.getVersion() > current.getVersion()) {
                            UpdateEventEntry<K, V> updated = new UpdateEventEntry<>(entry.getValue(), current.getOldValue());
                            log.info("Update from buffer {}: {} -> {}", entry.getRecordKey(), current.getVersion(), entry.getVersion());
                            entries.put(key, updated);
                        }
                    }
                });
        log.info("Buffered update events merged");
        return new UpdateEvent<>(entries.values(), UpdateEvent.Type.INCREMENTAL);
    }
}

