package com.grid.sandbox.core.service;

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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.TRADE_KEY_MAPPER;
import static com.grid.sandbox.utils.TestHelpers.*;
import static com.grid.sandbox.utils.CacheUtils.*;
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
        testTrades = generateTrades();
        snapshot = testTrades.stream()
                .collect(Collectors.toMap(Trade::getTradeId, trade -> createEventEntry(trade, null)));
    }

    @Test
    void testSnapshotNoComparators() throws Throwable {
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(
                feed, SAME_THREAD_SCHEDULER);
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
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, SAME_THREAD_SCHEDULER);
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
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Collections.singletonList(createEventEntry(trade15_upd, trade15)), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, SAME_THREAD_SCHEDULER);
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
        Flowable<UpdateEvent<String, Trade>> feed = Flowable.just(
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT),
                new UpdateEvent<>(Arrays.asList(
                        createEventEntry(setStatus(trade5, TradeStatus.CANCELLED), trade5),
                        createEventEntry(setStatus(trade4, TradeStatus.CANCELLED), trade4)
                ), UpdateEvent.Type.INCREMENTAL)
        );
        Comparator<Trade> comparator = new MultiComparator<>(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR);
        BlotterReportService<String, Trade> blotterReportService =
                new BlotterReportService<>(feed, SAME_THREAD_SCHEDULER);
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
