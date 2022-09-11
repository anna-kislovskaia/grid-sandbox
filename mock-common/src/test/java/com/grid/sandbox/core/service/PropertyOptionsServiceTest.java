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

import static com.grid.sandbox.utils.CacheUtils.*;
import static com.grid.sandbox.utils.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
@Log4j2
class PropertyOptionsServiceTest {
    private static final PropertyOptionsTracker<Trade> TRADE_FILTER_OPTION_BUILDER = getTradeFilterOptionBuilder();
    private List<Trade> testTrades ;
    private BlotterFeedService<String, Trade> blotterFeedService = new BlotterFeedService<>();
    {
        blotterFeedService.init();
    }

    @BeforeEach
    void init() {
        testTrades = generateTrades();
        blotterFeedService.reset(testTrades);
    }

    @Test
    public void testFilterTradeUpdated() throws Throwable {
        PropertyOptionsService<String, Trade> filterOptionService = new PropertyOptionsService<>(
                "feedId", blotterFeedService, SAME_THREAD_SCHEDULER, TRADE_FILTER_OPTION_BUILDER, ACCEPT_ALL
        );
        filterOptionService.subscribe();

        Map<String, Integer> validationMap = new HashMap<>();
        validationMap.put("client", 4);
        validationMap.put("status", 1);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);

        // updated and add
        Trade trade1 = testTrades.get(0).toBuilder().status(TradeStatus.CANCELLED).build();
        Trade trade16 = new Trade("16", BigDecimal.valueOf(250), "client 5", System.currentTimeMillis(), TradeStatus.DRAFT);
        blotterFeedService.update(Arrays.asList(trade16, trade1));

        validationMap.put("client", 5);
        validationMap.put("status", 3);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);
        filterOptionService.close();
    }

    @Test
    public void testFilterTradeRemoved() throws Throwable {
        PropertyOptionsService<String, Trade> filterOptionService = new PropertyOptionsService<>(
                "feedId", blotterFeedService, SAME_THREAD_SCHEDULER, TRADE_FILTER_OPTION_BUILDER, ACCEPT_OPENED
        );
        filterOptionService.subscribe();

        Map<String, Integer> validationMap = new HashMap<>();
        validationMap.put("client", 4);
        validationMap.put("status", 1);
        validatePropertyOptions(filterOptionService.getFilterOptions(), validationMap);

        // cancel all "client 3" trades
        List<Trade> updates = testTrades.stream()
                .filter(trade -> "client 3".equals(trade.getClient()))
                .map(trade -> trade.toBuilder().status(TradeStatus.CANCELLED).build())
                .toList();
        blotterFeedService.update(updates);

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
