package com.grid.sandbox.service;

import com.grid.sandbox.model.ClusterStateChangeEvent;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.UpdateEvent;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.replicatedmap.ReplicatedMap;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@Log4j2
public class TradeFeedService extends EntryAdapter<String, Trade> {
    private static final int REFRESH_INTERVAL = 10;
    private final AtomicReference<UUID> cacheSubId = new AtomicReference<>();
    private final BlockingQueue<EntryEvent<String, Trade>> eventQueue = new ArrayBlockingQueue<>(Character.MAX_VALUE);
    private final PublishSubject<UpdateEvent> tradeUpdates = PublishSubject.create();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Subject<ConcurrentMap<String, Trade>> snapshotPublisher = BehaviorSubject.create();
    private final Flowable<ConcurrentMap<String, Trade>> snapshotFlowable = snapshotPublisher.toFlowable(BackpressureStrategy.LATEST);
    private final AtomicLong lastUpdateTime = new AtomicLong();

    private Flowable<UpdateEvent>  updateEventFlowable;

    @Autowired
    private ReplicatedMap<String, Trade> tradeCache;

    @Autowired
    private ClusterLifecycleListenerService lifecycleListenerService;

    @PostConstruct
    private void init() {
        cacheSubId.set(tradeCache.addEntryListener(this));
        executor.execute(this::updateHandler);

        lifecycleListenerService.getClusterStateFeed().subscribe(lifecycleEvent -> {
            log.info("Cluster event {}", lifecycleEvent);

            if (lifecycleEvent.getType() != ClusterStateChangeEvent.Type.DISCONNECTED) {
                log.info("Loading snapshot...");
                lastUpdateTime.set(System.currentTimeMillis());
                ConcurrentMap<String, Trade> allTrades = tradeCache.values().stream()
                        .collect(Collectors.toConcurrentMap(Trade::getTradeId, event -> event));
                snapshotPublisher.onNext(allTrades);
                log.info("Snapshot loaded {}", allTrades.size());
            }
        });

        updateEventFlowable = snapshotFlowable.switchMap(snapshot -> {
                    Flowable<UpdateEvent> periodicUpdates = Flowable.interval(REFRESH_INTERVAL, TimeUnit.SECONDS)
                            .map(i -> {
                                log.info("Loading periodic updates");
                                long lastRefresh = lastUpdateTime.getAndSet(System.currentTimeMillis());
                                Map<String, EntryEvent<String, Trade>> updates = tradeCache.values().stream()
                                        .filter(trade -> trade.getLastUpdateTimestamp() >= lastRefresh)
                                        .map(TradeFeedService::createAddEvent)
                                        .collect(Collectors.toMap(EntryEvent::getKey, event -> event));
                                log.info("Refresh event: {}", updates.size());
                                return new UpdateEvent(updates, UpdateEvent.Type.INCREMENTAL);
                            })
                            .filter(event -> !event.getUpdates().isEmpty());

                    Flowable<UpdateEvent> quickUpdates = tradeUpdates.toFlowable(BackpressureStrategy.BUFFER);
                    return Flowable.merge(quickUpdates, periodicUpdates)
                            .map(event -> {
                                applyUpdateEvent(event, snapshot);
                                return event;
                            });
                }
        );
        updateEventFlowable.subscribe();
    }

    private void applyUpdateEvent(UpdateEvent event, ConcurrentMap<String, Trade> allTrades) {
        log.info("Apply update event: {}", event.toShortString());
        Map<String, EntryEvent<String, Trade>> updates = event.getUpdates();
        for (Iterator<Map.Entry<String, EntryEvent<String, Trade>>> iterator = updates.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, EntryEvent<String, Trade>> entry = iterator.next();
            Trade old = allTrades.get(entry.getKey());
            Trade updatedTrade = entry.getValue().getValue();
            if (updatedTrade != null) {
                if (old == null || old.getLastUpdateTimestamp() <= updatedTrade.getLastUpdateTimestamp()) {
                    log.info("Snapshot update {}", updatedTrade);
                    allTrades.put(entry.getKey(), updatedTrade);
                    entry.setValue(updateEvent(entry.getValue(), old, updatedTrade));
                } else {
                    log.info("Stale update {}", updatedTrade);
                    iterator.remove();
                }
            } else {
                log.info("Trade deleted {}: {}", entry.getKey(), old);
                allTrades.remove(entry.getKey());
            }
        }
    }

    @Override
    public void onEntryEvent(EntryEvent<String, Trade> event) {
        log.info("Cache event received " + event);
        eventQueue.add(event);
    }

    private void updateHandler() {
        log.info("Event handler started");
        try {
            while (true) {
                EntryEvent<String, Trade> event = eventQueue.take();
                ArrayList<EntryEvent<String, Trade>> updates = new ArrayList<>();
                updates.add(event);
                eventQueue.drainTo(updates);
                Map<String, EntryEvent<String, Trade>> events = new HashMap<>();
                updates.forEach(updateEvent -> events.put(updateEvent.getKey(), updateEvent));
                UpdateEvent updateEvent = new UpdateEvent(events, UpdateEvent.Type.INCREMENTAL);
                log.info("Event update published: {}", updateEvent);
                tradeUpdates.onNext(updateEvent);
            }
        } catch (InterruptedException e) {
            log.error("Error happens while listening to cache update event queue", e);
        }
        log.info("Event handler stopped");
    }

    public Flowable<UpdateEvent> getTradeFeed() {
        Flowable<UpdateEvent> snapshotFeed = snapshotPublisher
                .toFlowable(BackpressureStrategy.LATEST)
                .map(snapshot -> {
                    Map<String, EntryEvent<String, Trade>> eventSnapshot = snapshot.values().stream()
                            .map(TradeFeedService::createAddEvent)
                            .collect(Collectors.toMap(EntryEvent::getKey, event -> event));
                    return new UpdateEvent(eventSnapshot, UpdateEvent.Type.SNAPSHOT);
                });

        return snapshotFeed.switchMap(snapshot -> {
            Flowable<UpdateEvent> snapshotEvent = Flowable.just(snapshot);
            return Flowable.concat(snapshotEvent, updateEventFlowable);
        });
    }

    private static EntryEvent<String, Trade> updateEvent(EntryEvent<String, Trade> event, Trade old, Trade updated) {
        return new EntryEvent<String, Trade>(event.getSource(), event.getMember(), event.getEventType().getType(), event.getKey(), old, updated);
    }

    private static EntryEvent<String, Trade> createAddEvent(Trade updated) {
        return new EntryEvent<String, Trade>("tradeCache", null, EntryEventType.ADDED.getType(), updated.getTradeId(), updated);
    }

    @PreDestroy
    private void unsubscribe() {
        UUID subscritpionId = cacheSubId.get();
        if(subscritpionId != null && cacheSubId.compareAndSet(subscritpionId, null)) {
            tradeCache.removeEntryListener(subscritpionId);
        }
    }

}
