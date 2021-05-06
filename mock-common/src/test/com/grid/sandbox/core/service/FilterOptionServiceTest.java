package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.FilterOptionUpdateEntry;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.model.*;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.BehaviorSubject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.*;
import static com.grid.sandbox.utils.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FilterOptionServiceTest {
    private static final FilterOptionBuilder<Trade> TRADE_FILTER_OPTION_BUILDER = getTradeFilterOptionBuilder();
    private List<Trade> testTrades ;
    private Map<String, UpdateEventEntry<String, Trade>> snapshot;
    @Mock
    private Consumer<List<FilterOptionUpdateEntry>> consumer;
    @Captor
    private ArgumentCaptor<List<FilterOptionUpdateEntry>> filterResetCaptor;

    @BeforeEach
    void init() {
        testTrades = generateTrades();
        snapshot = testTrades.stream()
                .collect(Collectors.toMap(Trade::getTradeId, trade -> createEventEntry(trade, null)));
    }

    @Test
    public void testFilterTradeUpdated() throws Throwable {
        BehaviorSubject<UpdateEvent<String, Trade>> subject = BehaviorSubject.create();
        Flowable<UpdateEvent<String, Trade>> feed = subject.toFlowable(BackpressureStrategy.LATEST).startWith(
                Flowable.fromSupplier(() -> new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT))
        );

        FilterOptionService<String, Trade> filterOptionService = new FilterOptionService<>(
                feed, TRADE_FILTER_OPTION_BUILDER, ACCEPT_ALL, SAME_THREAD_SCHEDULER
        );
        filterOptionService.getFilterOptions().subscribe(consumer);

        // updated and add
        Trade trade1 = snapshot.get("1").getValue().toBuilder().status(TradeStatus.CANCELLED).build();
        Trade trade16 = new Trade("16", BigDecimal.valueOf(250), "client 5", System.currentTimeMillis(), TradeStatus.DRAFT);
        snapshot.put("1", createEventEntry(trade1, snapshot.get("1").getValue()));
        snapshot.put("16", createEventEntry(trade16, null));
        subject.onNext(new UpdateEvent<>(Arrays.asList(snapshot.get("1"), snapshot.get("16")), UpdateEvent.Type.INCREMENTAL));

        verify(consumer, times(2)).accept(filterResetCaptor.capture());
        List<FilterOptionUpdateEntry> initial = filterResetCaptor.getAllValues().get(0);
        assertEquals(2, initial.size());
        // client names
        assertEquals(4, initial.get(0).getOptions().size());
        // statuses
        assertEquals(1, initial.get(1).getOptions().size());

        List<FilterOptionUpdateEntry> updated = filterResetCaptor.getAllValues().get(1);
        assertEquals(2, updated.size());
        // client names
        assertEquals(5, updated.get(0).getOptions().size());
        // statuses
        assertEquals(3, updated.get(1).getOptions().size());
    }

    @Test
    public void testFilterTradeRemoved() throws Throwable {
        BehaviorSubject<UpdateEvent<String, Trade>> subject = BehaviorSubject.create();
        Flowable<UpdateEvent<String, Trade>> feed = subject.toFlowable(BackpressureStrategy.LATEST).startWith(
                Flowable.fromSupplier(() -> new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT))
        );

        FilterOptionService<String, Trade> filterOptionService = new FilterOptionService<>(
                feed, TRADE_FILTER_OPTION_BUILDER, ACCEPT_OPENED, SAME_THREAD_SCHEDULER
        );
        filterOptionService.getFilterOptions().subscribe(consumer);

        // cancel all "client 3" trades
        List<Trade> client3 = snapshot.values().stream()
                .filter(event -> "client 3".equals(event.getValue().getClient()))
                .map(UpdateEventEntry::getValue)
                .collect(Collectors.toList());
        List<UpdateEventEntry<String, Trade>> updates = client3.stream().map(trade -> {
            Trade cancelled = trade.toBuilder().status(TradeStatus.CANCELLED).build();
            UpdateEventEntry<String, Trade> entry = createEventEntry(cancelled, trade);
            snapshot.put(trade.getTradeId(), entry);
            return entry;
        }).collect(Collectors.toList());
        subject.onNext(new UpdateEvent<>(updates, UpdateEvent.Type.INCREMENTAL));

        verify(consumer, times(2)).accept(filterResetCaptor.capture());
        List<FilterOptionUpdateEntry> initial = filterResetCaptor.getAllValues().get(0);
        assertEquals(2, initial.size());
        // client names
        assertEquals(4, initial.get(0).getOptions().size());
        // statuses
        assertEquals(1, initial.get(1).getOptions().size());

        List<FilterOptionUpdateEntry> updated = filterResetCaptor.getAllValues().get(1);
        assertEquals(2, updated.size());
        // client names
        assertEquals(3, updated.get(0).getOptions().size());
        // statuses
        assertEquals(1, updated.get(1).getOptions().size());
    }
}
