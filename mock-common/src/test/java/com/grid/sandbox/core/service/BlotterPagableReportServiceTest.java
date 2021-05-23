package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.BlotterViewport;
import com.grid.sandbox.core.model.PageUpdate;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.core.utils.MultiComparator;
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

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.TestHelpers.*;
import static com.grid.sandbox.utils.CacheUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BlotterPagableReportServiceTest {
    private final BlotterViewport viewport = new BlotterViewport(1, 4);
    private final Flowable<BlotterViewport> request = Flowable.just(viewport);

    private List<Trade> testTrades;
    private Map<String, UpdateEventEntry<String, Trade>> snapshot;
    @Mock
    private Consumer<PageUpdate> consumer;
    @Captor
    private ArgumentCaptor<PageUpdate<Trade>> pageUpdateCaptor;

    @BeforeEach
    void init() {
        testTrades = generateTrades();
        snapshot = testTrades.stream()
                .collect(Collectors.toMap(Trade::getTradeId, trade -> createEventEntry(trade, null)));
    }

    @Test
    void testSnapshotNoComparators() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_ALL, ID_COMPARATOR);
        blotterReportService.getReport(feed, request).subscribe(consumer);

        verify(consumer, times(1)).accept(pageUpdateCaptor.capture());
        PageUpdate<Trade> snapshotUpdate = pageUpdateCaptor.getValue();
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(snapshotUpdate.getTotalSize(), testTrades.size());
        assertEquals(snapshotUpdate.getUpdated().size(), viewport.getPageSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());
        String[] expected = new String[]{"13", "14", "15", "2"};
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());
    }

    @Test
    void testSnapshotClientBalanceComparator() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_ALL, comparator);
        blotterReportService.getReport(feed, request).subscribe(consumer);

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
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Collections.singletonList(createEventEntry(trade15_upd, trade15)), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_ALL, comparator);
        blotterReportService.getReport(feed, request).subscribe(consumer);

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
        Trade trade5 = getTrade("5");
        // client 2 trade within page 1
        Trade trade4 = getTrade("4");
        Trade trade12 = getTrade("12");
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Arrays.asList(
                        createEventEntry(setStatus(trade5, TradeStatus.CANCELLED), trade5),
                        createEventEntry(setStatus(trade4, TradeStatus.CANCELLED), trade4)
                ), UpdateEvent.Type.INCREMENTAL),
                new UpdateEvent<>(Collections.singletonList(
                        createEventEntry(setStatus(trade12, TradeStatus.CANCELLED), trade12)
                ), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        // initial state by comparator
        // client 1: 5, 11, 1, 2, 14, 15
        // client 2: 4, 3, 12
        // client 3: 9, 13
        // client 4: 6, 7, 10
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_OPENED, comparator);
        blotterReportService.getReport(feed, request).subscribe(consumer);

        verify(consumer, times(3)).accept(pageUpdateCaptor.capture());
        List<PageUpdate<Trade>> events = pageUpdateCaptor.getAllValues();
        PageUpdate<Trade> snapshotUpdate = events.get(0);
        assertTrue(snapshotUpdate.isSnapshot());
        assertEquals(testTrades.size(), snapshotUpdate.getTotalSize());
        assertTrue(snapshotUpdate.getDeleted().isEmpty());

        String[] expected = new String[]{ /*client 1*/ "2", "14", "15", /*client 2*/ "4" };
        assertArrayEquals(expected, snapshotUpdate.getUpdated().stream().map(Trade::getTradeId).toArray());

        // 2 new rows + 2 deleted rows for 4 page size -> page snapshot returned
        PageUpdate<Trade> resetUpdate = events.get(1);
        assertTrue(resetUpdate.isSnapshot());
        assertEquals(testTrades.size() - 2, resetUpdate.getTotalSize());

        // after  trade cancel expected page state
        // String[] expected = new String[]{ /*client 1*/ "14", "15", /*client 2*/ "3", "12" };
        assertTrue(resetUpdate.getDeleted().isEmpty());
        assertArrayEquals(new Trade[]{getTrade("14"), getTrade("15"), getTrade("3"), getTrade("12")}, resetUpdate.getUpdated().toArray());

        // 1 new rows + 1 deleted rows for 4 page size -> page incremental returned
        PageUpdate<Trade> incrementalUpdate = events.get(2);
        assertFalse(incrementalUpdate.isSnapshot());
        assertEquals(testTrades.size() - 3, incrementalUpdate.getTotalSize());

        // after  trade cancel expected page state
        // String[] expected = new String[]{ /*client 1*/ "14", "15", /*client 2*/ "3", "9" };
        assertArrayEquals(new Trade[]{trade12}, incrementalUpdate.getDeleted().toArray());
        assertArrayEquals(new Trade[]{getTrade("9")}, incrementalUpdate.getUpdated().toArray());
    }

    private Trade getTrade(String tradeId) {
        return snapshot.get(tradeId).getValue();
    }
}
