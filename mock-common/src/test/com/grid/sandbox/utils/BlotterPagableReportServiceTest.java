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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.TRADE_KEY_MAPPER;
import static com.grid.sandbox.utils.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BlotterPagableReportServiceTest {
    private final Pageable request = PageRequest.of(1, 4);

    private List<Trade> testTrades;
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
                .collect(Collectors.toMap(Trade::getTradeId, trade -> new UpdateEventEntry<>(trade.getTradeId(), trade, null)));
    }

    @Test
    void testSnapshotNoComparators() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(snapshot, UpdateEvent.Type.SNAPSHOT)
        );
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(
                feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_ALL, ID_COMPARATOR).subscribe(consumer);


        verify(consumer, times(1)).accept(pageUpdateCaptor.capture());
        PageUpdate<Trade> snapshotUpdate = pageUpdateCaptor.getValue();
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(snapshotUpdate.getTotalSize(), testTrades.size());
        assertEquals(snapshotUpdate.getUpdated().size(), request.getPageSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());
        String[] expected = new String[]{"13", "14", "15", "2"};
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());
    }

    @Test
    void testSnapshotClientBalanceComparator() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(snapshot, UpdateEvent.Type.SNAPSHOT)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_ALL, comparator).subscribe(consumer);


        verify(consumer, times(1)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> updates = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = updates.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        String[] expected = new String[]{ /*client 1*/ "2", "14", "15", /*client 2*/ "4" };
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());
    }

    @Test
    void testSnapshotClientBalanceComparatorUpdate() throws Throwable {
        // remove 15 from page
        Trade trade15 = snapshot.get("15").getValue();
        Trade trade15_upd = trade15.toBuilder().balance(BigDecimal.valueOf(120)).build();
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(snapshot, UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Collections.singletonMap("15", new UpdateEventEntry<>("15", trade15_upd, trade15)), UpdateEvent.Type.INCREMENTAL)
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

        String[] expected = new String[]{ /*client 1*/ "2", "14", "15", /*client 2*/ "4" };
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());

        PageUpdate<Trade> incrementalUpdate = updates.get(1);
        Trade expectedReplace = snapshot.get("1").getValue();
        assertArrayEquals(new Trade[]{trade15}, incrementalUpdate.getDeleted().toArray());
        assertArrayEquals(new Trade[]{expectedReplace}, incrementalUpdate.getUpdated().toArray());
    }

    @Test
    void testRemoveByFilter() throws Throwable {
        // client 1 trade out of page 1
        Trade trade5 = snapshot.get("5").getValue();
        // client 2 trade within page 1
        Trade trade4 = snapshot.get("4").getValue();
        Map<String, UpdateEventEntry<String, Trade>> updates = new HashMap<>();
        updates.put(trade5.getTradeId(), createEventEntry(setStatus(trade5, TradeStatus.CANCELLED), trade5));
        updates.put(trade4.getTradeId(), createEventEntry(setStatus(trade4, TradeStatus.CANCELLED), trade4));
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(snapshot, UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(updates, UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, TRADE_KEY_MAPPER, SAME_THREAD_SCHEDULER);
        blotterReportService.getReport(request, ACCEPT_OPENED, comparator).subscribe(consumer);

        verify(consumer, times(2)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> events = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = events.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        String[] expected = new String[]{ /*client 1*/ "2", "14", "15", /*client 2*/ "4" };
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());

        PageUpdate<Trade> incrementalUpdate = events.get(1);
        assertFalse(incrementalUpdate.isSnapshot());
        assertEquals(testTrades.size() - 2, incrementalUpdate.getTotalSize());

        // after  trade cancel expected page state
        // String[] expected = new String[]{ /*client 1*/ "14", "15", /*client 2*/ "3", "12" };
        assertArrayEquals(new Trade[]{snapshot.get("2").getValue(), trade4}, incrementalUpdate.getDeleted().toArray());
        assertArrayEquals(new Trade[]{snapshot.get("3").getValue(), snapshot.get("12").getValue()}, incrementalUpdate.getUpdated().toArray());
    }
}
