package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.TradeStatus;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.grid.sandbox.utils.TestHelpers.SAME_THREAD_SCHEDULER;
import static com.grid.sandbox.utils.TestHelpers.generateTrades;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Log4j2
class BlotterFeedServiceTest {

    private List<Trade> testTrades;
    private BlotterFeedService<String, Trade> feedService;
    @Mock
    private Consumer<UpdateEvent<String, Trade>> consumer;
    @Captor
    private ArgumentCaptor<UpdateEvent<String, Trade>> eventCaptor;

    @BeforeEach
    void setup() {
        testTrades = generateTrades();
        feedService = new BlotterFeedService<>();
        feedService.init();
    }

    @Test
    void testResetUpdateSubscription() throws Throwable {
        feedService.getFeed(SAME_THREAD_SCHEDULER)
                .doOnNext(event -> log.info(event.toShortString()))
                .subscribe(consumer);
        Trade original = testTrades.get(0);
        Trade update = original.toBuilder()
                .lastUpdateTimestamp(Long.MAX_VALUE)
                .status(TradeStatus.CANCELLED)
                .build();
        feedService.update(Collections.singleton(update));

        feedService.reset(testTrades);
        verify(consumer, times(1)).accept(eventCaptor.capture());
        assertEquals(1, eventCaptor.getAllValues().size());
        UpdateEvent<String, Trade> event = eventCaptor.getValue();
        assertTrue(event.isSnapshot());
        Trade testTrade = eventCaptor.getValue().getUpdates().stream()
                .map(UpdateEventEntry::getValue)
                .filter(trade -> update.getTradeId().equals(trade.getTradeId()))
                .findAny().get();
        assertEquals(original.getLastUpdateTimestamp(), testTrade.getLastUpdateTimestamp());
        assertEquals(original.getStatus(), testTrade.getStatus());
    }

    @Test
    void testSkipPreviousVersionUpdate() throws Throwable {
        feedService.reset(testTrades);
        feedService.getFeed(SAME_THREAD_SCHEDULER)
                .doOnNext(event -> log.info(event.toShortString()))
                .subscribe(consumer);

        // stale update
        Trade original = testTrades.get(0);
        Trade update = original.toBuilder()
                .lastUpdateTimestamp(0)
                .status(TradeStatus.CANCELLED)
                .build();
        feedService.update(Collections.singleton(update));

        verify(consumer, times(1)).accept(eventCaptor.capture());
        assertEquals(1, eventCaptor.getAllValues().size());
        UpdateEvent<String, Trade> event = eventCaptor.getValue();
        assertTrue(event.isSnapshot());
    }

    @Test
    void testApplyLatestUpdate() throws Throwable {
        feedService.reset(testTrades);
        feedService.getFeed(SAME_THREAD_SCHEDULER)
                .doOnNext(event -> log.info(event.toShortString()))
                .subscribe(consumer);

        // stale update
        Trade original = testTrades.get(0);
        Trade updateStale = original.toBuilder()
                .lastUpdateTimestamp(Long.MAX_VALUE - 1)
                .status(TradeStatus.CANCELLED)
                .build();
        Trade update = original.toBuilder()
                .lastUpdateTimestamp(Long.MAX_VALUE)
                .status(TradeStatus.REJECTED)
                .build();
        feedService.update(Arrays.asList(updateStale, update));

        verify(consumer, times(2)).accept(eventCaptor.capture());
        UpdateEvent<String, Trade> event = eventCaptor.getValue();
        assertFalse(event.isSnapshot());
        assertEquals(1, event.getUpdates().size());
        assertEquals(Long.MAX_VALUE, event.getUpdates().iterator().next().getValue().getLastUpdateTimestamp());
    }

    @RepeatedTest(3)
    void testGetAllNewFeed() throws Throwable {
        feedService.reset(testTrades);
        int statusCount = TradeStatus.values().length;
        int startIndex = testTrades.size() + 1;
        String[] clients = {"Client 2", "Client 3", "Client 4"};
        Disposable updateSubscription = Flowable.interval(1, TimeUnit.MICROSECONDS).forEach(i -> {
            long nextTradeId = startIndex + i;
            Trade update = Trade.builder()
                    .tradeId("" + nextTradeId)
                    .balance(BigDecimal.valueOf(100))
                    .lastUpdateTimestamp(System.currentTimeMillis())
                    .client(clients[i.intValue() % clients.length])
                    .status(TradeStatus.values()[i.intValue() % statusCount])
                    .build();
            feedService.update(Collections.singleton(update));
        });

        List<UpdateEvent<String, Trade>> allEvents = new CopyOnWriteArrayList<>();
        feedService.getFeed(Schedulers.single())
                .doOnNext(event -> log.info(event.toShortString()))
                .subscribe(allEvents::add);

        Thread.sleep(100);

        updateSubscription.dispose();
        Thread.sleep(500);

        // test events applicable
        Map<String, Trade> snapshot = new HashMap<>();
        for(UpdateEvent<String, Trade> event : allEvents) {
            if (event.isSnapshot()) {
                snapshot.clear();
                event.getUpdates().stream().map(UpdateEventEntry::getValue)
                        .forEach(value -> snapshot.put(value.getRecordKey(), value));
            } else {
                event.getUpdates().forEach(entry -> {
                    if (entry.getOldValue() != null) {
                        assertSame(entry.getOldValue(), snapshot.get(entry.getRecordKey()));
                    }
                    snapshot.put(entry.getRecordKey(), entry.getValue());
                });
            }
        }
    }

    @RepeatedTest(15)
    void testGetUpdatedFeed() throws Throwable {
        feedService.reset(testTrades);
        int statusCount = TradeStatus.values().length;
        int startIndex = testTrades.size() + 1;
        String[] clients = {"Client 2", "Client 3", "Client 4"};
        Disposable updateSubscription = Flowable.interval(100, TimeUnit.MICROSECONDS).forEach(i -> {
            long nextNewTradeId = startIndex + i;
            long tradeId = i > 0 && i % 3 == 0 ? nextNewTradeId / 2 : nextNewTradeId;
            Trade update = Trade.builder()
                    .tradeId("" + tradeId)
                    .balance(BigDecimal.valueOf(100))
                    .lastUpdateTimestamp(System.currentTimeMillis())
                    .client(clients[i.intValue() % clients.length])
                    .status(TradeStatus.values()[i.intValue() % statusCount])
                    .build();
            feedService.update(Collections.singleton(update));
        });

        List<UpdateEvent<String, Trade>> allEvents = new CopyOnWriteArrayList<>();
        feedService.getFeed(Schedulers.single())
                .doOnNext(event -> log.info(event.toShortString()))
                .subscribe(allEvents::add);

        Thread.sleep(100);

        // close updater and wait for all events
        updateSubscription.dispose();
        Thread.sleep(1000);

        // test events applicable
        Map<String, Trade> snapshot = new HashMap<>();
        for(UpdateEvent<String, Trade> event : allEvents) {
            if (event.isSnapshot()) {
                snapshot.clear();
                event.getUpdates().stream().map(UpdateEventEntry::getValue)
                        .forEach(value -> snapshot.put(value.getRecordKey(), value));
            } else {
                event.getUpdates().forEach(entry -> {
                    if (entry.getOldValue() != null) {
                        assertSame(entry.getOldValue(), snapshot.get(entry.getRecordKey()));
                    }
                    snapshot.put(entry.getRecordKey(), entry.getValue());
                });
            }
        }

        UpdateEvent<String, Trade> event = feedService.getSnapshotFeed().take(1)
                .subscribeOn(SAME_THREAD_SCHEDULER)
                .toList()
                .blockingGet()
                .get(0);
        assertEquals(event.getUpdates().size(), snapshot.size());
        event.getUpdates().stream().map(UpdateEventEntry::getValue)
                .forEach(value -> assertSame(value, snapshot.get(value.getRecordKey())));
    }
}
