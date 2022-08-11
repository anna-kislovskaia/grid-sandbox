package com.grid.sandbox.core.service;

import com.grid.sandbox.core.model.PropertyOptionsUpdateEntry;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.grid.sandbox.model.*;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.BehaviorSubject;
import lombok.extern.log4j.Log4j2;
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

import static com.grid.sandbox.utils.CacheUtils.*;
import static com.grid.sandbox.utils.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
@Log4j2
class PropertyOptionsServiceTest {
    private static final PropertyOptionsTracker<Trade> TRADE_FILTER_OPTION_BUILDER = getTradeFilterOptionBuilder();
    private List<Trade> testTrades ;
    private Map<String, UpdateEventEntry<String, Trade>> snapshot;
    @Mock
    private Consumer<Collection<PropertyOptionsUpdateEntry>> consumer;
    @Captor
    private ArgumentCaptor<List<PropertyOptionsUpdateEntry>> filterResetCaptor;

    private Flowable<UpdateEvent<String, Trade>> snapshotFeed;

    @BeforeEach
    void init() {
        testTrades = generateTrades();
        snapshot = testTrades.stream()
                .collect(Collectors.toMap(Trade::getTradeId, trade -> createEventEntry(trade, null)));
        snapshotFeed = Flowable.fromSupplier(() ->
                new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT));
    }

    @Test
    public void testFilterTradeUpdated() throws Throwable {
        BehaviorSubject<UpdateEvent<String, Trade>> subject = BehaviorSubject.create();
        Flowable<UpdateEvent<String, Trade>> feed = subject.toFlowable(BackpressureStrategy.LATEST).startWith(snapshotFeed);
        PropertyOptionsService<String, Trade> filterOptionService = new PropertyOptionsService<>(
                feed, snapshotFeed, TRADE_FILTER_OPTION_BUILDER, ACCEPT_ALL
        );
        filterOptionService.subscribe();

        Map<String, Integer> validationMap = new HashMap<>();
        validationMap.put("client", 4);
        validationMap.put("status", 1);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);

        // updated and add
        Trade trade1 = snapshot.get("1").getValue().toBuilder().status(TradeStatus.CANCELLED).build();
        Trade trade16 = new Trade("16", BigDecimal.valueOf(250), "client 5", System.currentTimeMillis(), TradeStatus.DRAFT);
        snapshot.put("1", createEventEntry(trade1, snapshot.get("1").getValue()));
        snapshot.put("16", createEventEntry(trade16, null));
        subject.onNext(new UpdateEvent<>(Arrays.asList(snapshot.get("1"), snapshot.get("16")), UpdateEvent.Type.INCREMENTAL));

        validationMap.put("client", 5);
        validationMap.put("status", 3);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);
        filterOptionService.close();
    }

    @Test
    public void testFilterTradeRemoved() throws Throwable {
        BehaviorSubject<UpdateEvent<String, Trade>> subject = BehaviorSubject.create();
        Flowable<UpdateEvent<String, Trade>> snapshotFeed =
                Flowable.fromSupplier(() -> new UpdateEvent<>(new ArrayList<>(snapshot.values()), UpdateEvent.Type.SNAPSHOT));
        Flowable<UpdateEvent<String, Trade>> feed = subject.toFlowable(BackpressureStrategy.LATEST).startWith(snapshotFeed);

        PropertyOptionsService<String, Trade> filterOptionService = new PropertyOptionsService<>(
                feed, snapshotFeed, TRADE_FILTER_OPTION_BUILDER, ACCEPT_OPENED
        );
        filterOptionService.subscribe();

        Map<String, Integer> validationMap = new HashMap<>();
        validationMap.put("client", 4);
        validationMap.put("status", 1);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);

        // cancel all "client 3" trades
        List<UpdateEventEntry<String, Trade>> updates = snapshot.values().stream()
                .filter(event -> "client 3".equals(event.getValue().getClient()))
                .map(UpdateEventEntry::getValue)
                .map(trade -> {
                    Trade updated = trade.toBuilder().status(TradeStatus.CANCELLED).build();
                    snapshot.put(updated.getRecordKey(), createEventEntry(updated, null));
                    return createEventEntry(updated, trade);
                })
                .collect(Collectors.toList());
        subject.onNext(new UpdateEvent<>(updates, UpdateEvent.Type.INCREMENTAL));

        validationMap.put("client", 3);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);
    }

    private void validatePropertyOptions(Collection<PropertyOptionsUpdateEntry> options, Map<String, Integer> expected) {
        log.info("Validating filter options: {} {}", options, expected);
        assertEquals(expected.size(), options.size());
        for (PropertyOptionsUpdateEntry propertyOptions : options) {
            assertEquals(expected.get(propertyOptions.getName()), propertyOptions.getOptions().size());
        }
    }
}
