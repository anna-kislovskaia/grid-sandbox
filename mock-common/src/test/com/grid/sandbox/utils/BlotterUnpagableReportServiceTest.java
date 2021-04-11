package com.grid.sandbox.utils;

import com.grid.sandbox.controller.UnpagedRequest;
import com.grid.sandbox.model.*;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static com.grid.sandbox.utils.TestHelpers.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.TRADE_KEY_MAPPER;

@ExtendWith(MockitoExtension.class)
class BlotterUnpagableReportServiceTest {
    private final Pageable request = new UnpagedRequest();
    private List<Trade> testTrades ;
    private Map<String, UpdateEventEntry<String, Trade>> snapshot;
    @Mock
    private Consumer<PageUpdate> consumer;
    @Captor
    private ArgumentCaptor<PageUpdate<Trade>> pageUpdateCaptor;

    @BeforeEach
    void init() {
        testTrades = new ArrayList<>();

        testTrades.add(new Trade("1",  BigDecimal.valueOf(500), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("2",  BigDecimal.valueOf(600), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("3",  BigDecimal.valueOf(533), "client 2", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("4",  BigDecimal.valueOf(500), "client 2", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("5",  BigDecimal.valueOf(100), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("6",  BigDecimal.valueOf(200), "client 4", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("7",  BigDecimal.valueOf(300), "client 4", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("8",  BigDecimal.valueOf(400), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("9",  BigDecimal.valueOf(500), "client 3", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("10", BigDecimal.valueOf(500), "client 4", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("11", BigDecimal.valueOf(220), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("12", BigDecimal.valueOf(560), "client 2", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("13", BigDecimal.valueOf(700), "client 3", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("14", BigDecimal.valueOf(800), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));
        testTrades.add(new Trade("15", BigDecimal.valueOf(900), "client 1", System.currentTimeMillis(), TradeStatus.PLACED));

        snapshot = testTrades.stream()
                .collect(Collectors.toMap(Trade::getTradeId, trade -> createEventEntry(trade, null)));
    }

    @Test
    void testSnapshotNoComparators() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(
                feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_ALL, ID_COMPARATOR).subscribe(consumer);


        verify(consumer, times(1)).accept(pageUpdateCaptor.capture());
        PageUpdate<Trade> snapshotUpdate = pageUpdateCaptor.getValue();
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(snapshotUpdate.getTotalSize(), testTrades.size());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());
    }

    @Test
    void testSnapshotClientBalanceComparator() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(new UnpagedRequest(), ACCEPT_ALL, comparator).subscribe(consumer);


        verify(consumer, times(1)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> updates = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = updates.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        String[] expected = new String[]{
                /*client 1*/ "5", "11", "8", "1", "2", "14", "15",
                /*client 2*/ "4", "3", "12",
                /*client 3*/ "9", "13",
                /*client 4*/ "6", "7", "10"};
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());
    }

    @Test
    void testSnapshotClientBalanceComparatorUpdate() throws Throwable {
        Trade client1_1 = snapshot.get("1").getValue();
        Trade client1_1_upd = client1_1.toBuilder().balance(BigDecimal.valueOf(700)).build();
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Collections.singletonList(createEventEntry(client1_1_upd, client1_1)), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_ALL, comparator).subscribe(consumer);

        verify(consumer, times(2)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> updates = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = updates.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        PageUpdate<Trade> incrementalUpdate = updates.get(1);
        assertArrayEquals(new Trade[]{client1_1_upd}, incrementalUpdate.getUpdated().toArray());
    }

    @Test
    void testRemoveByFilter() throws Throwable {
        Trade trade10 = snapshot.get("10").getValue();
        Trade trade10_upd = trade10.toBuilder().status(TradeStatus.CANCELLED).build();
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Collections.singletonList(createEventEntry(trade10_upd, trade10)), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_OPENED, comparator).subscribe(consumer);

        verify(consumer, times(2)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> updates = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = updates.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        PageUpdate<Trade> incrementalUpdate = updates.get(1);
        assertFalse(incrementalUpdate.isSnapshot());
        assertEquals(testTrades.size() - 1, incrementalUpdate.getTotalSize());
        assertTrue(incrementalUpdate.getUpdated().isEmpty());
        assertArrayEquals(new Trade[]{trade10}, incrementalUpdate.getDeleted().toArray());
    }
}
