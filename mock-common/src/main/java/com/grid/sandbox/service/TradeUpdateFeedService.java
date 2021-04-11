package com.grid.sandbox.service;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.UpdateEvent;
import com.grid.sandbox.model.UpdateEventEntry;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;
import lombok.extern.log4j.Log4j2;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Log4j2
public class TradeUpdateFeedService extends EntryAdapter<String, Trade> {
    private final PublishSubject<UpdateEvent<String, Trade>> tradeUpdates = PublishSubject.create();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<EntryEvent<String, Trade>> eventQueue = new ArrayBlockingQueue<>(1_000_000);

    @PostConstruct
    public void init() {
        executor.execute(this::updateHandler);
    }

    @Override
    public void onEntryEvent(EntryEvent<String, Trade> event) {
        log.debug("Cache event received {}", event);
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
                List<UpdateEventEntry<String, Trade>> events = updates.stream()
                        .map(updateEvent ->
                            new UpdateEventEntry<>(updateEvent.getKey(), updateEvent.getValue(), updateEvent.getOldValue()))
                        .collect(Collectors.toList());
                UpdateEvent<String, Trade> updateEvent = new UpdateEvent<>(events, UpdateEvent.Type.INCREMENTAL);
                log.info("Event update published: {}", updateEvent);
                tradeUpdates.onNext(updateEvent);
            }
        } catch (InterruptedException e) {
            log.error("Error happens while listening to cache update event queue", e);
        }
        log.info("Event handler stopped");
    }

    public Flowable<UpdateEvent<String, Trade>> getTradeUpdateFeed() {
        return tradeUpdates.toFlowable(BackpressureStrategy.BUFFER);
    }
}
