package com.grid.sandbox.service;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.UpdateEvent;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.replicatedmap.ReplicatedMap;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@Log4j2
public class TradeFeedService extends EntryAdapter<String, Trade> {
    private static final int REFRESH_INTERVAL = 30;
    private final AtomicReference<UUID> cacheSubId = new AtomicReference<>();
    private final BlockingQueue<EntryEvent<String, Trade>> eventQueue = new ArrayBlockingQueue<>(Character.MAX_VALUE);
    private final PublishSubject<UpdateEvent> tradeUpdates = PublishSubject.create();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Autowired
    private ReplicatedMap<String, Trade> tradeCache;

    @PostConstruct
    private void init() {
        cacheSubId.set(tradeCache.addEntryListener(this));
        executor.execute(this::updateHandler);
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
                Map<String, EntryEvent<String, Trade>> events = updates.stream().collect(
                        Collectors.toMap(EntryEvent::getKey, entryEvent -> entryEvent));
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
        ConnectableFlowable<UpdateEvent> updatesFlowable = tradeUpdates
                .subscribeOn(Schedulers.computation())
                .toFlowable(BackpressureStrategy.BUFFER)
                .replay(REFRESH_INTERVAL, TimeUnit.SECONDS);
        Disposable disposable = updatesFlowable.subscribe();

        Map<String, EntryEvent<String, Trade>> tradeSnapshot = tradeCache.entrySet().stream()
                .map(entry -> new EntryEvent<String, Trade>("tradeCache", null, EntryEventType.ADDED.getType(), entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(EntryEvent::getKey, event -> event));
        Flowable<UpdateEvent> snapshotFlowable = Flowable.just(new UpdateEvent(tradeSnapshot, UpdateEvent.Type.SNAPSHOT));

        return Flowable.merge(snapshotFlowable, updatesFlowable)
                .map(event -> {
                    log.info("Received " + event);
                    if (event.getType() == UpdateEvent.Type.SNAPSHOT) {
                        log.info("Switch on updates");
                        updatesFlowable.connect();
                    }
                    return event;
                }).doOnError(e -> {
                    if (!disposable.isDisposed()) {
                        disposable.dispose();
                    }
                });
    }

    private void unsubscribe() {
        UUID subscritpionId = cacheSubId.get();
        if(subscritpionId != null && cacheSubId.compareAndSet(subscritpionId, null)) {
            tradeCache.removeEntryListener(subscritpionId);
        }
    }

}
