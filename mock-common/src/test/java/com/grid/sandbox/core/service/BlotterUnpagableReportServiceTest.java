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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static com.grid.sandbox.utils.TestHelpers.*;
import static com.grid.sandbox.utils.CacheUtils.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@ExtendWith(MockitoExtension.class)
class BlotterUnpagableReportServiceTest {
    private final Flowable<BlotterViewport> request = Flowable.just(BlotterViewport.UNPAGED);
    private List<Trade> testTrades ;
    private Map<String, UpdateEventEntry<String, Trade>> snapshot;
    @Mock
    private Consumer<PageUpdate<Trade>> consumer;
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
        assertTrue(snapshotUpdate.getDeleted().isEmpty());
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
        Comparator<Trade> comparator = new MultiComparator<>(Arrays.asList(
                Comparator.comparing(Trade::getClient),
                Comparator.comparing(Trade::getBalance),
                ID_COMPARATOR));
        BlotterReportService<String, Trade> blotterReportService = new BlotterReportService<>(ACCEPT_OPENED, comparator);
        blotterReportService.getReport(feed, request).subscribe(consumer);

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
