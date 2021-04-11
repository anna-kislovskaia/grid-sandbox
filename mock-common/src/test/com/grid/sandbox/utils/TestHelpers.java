package com.grid.sandbox.utils;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.TradeStatus;
import com.grid.sandbox.model.UpdateEventEntry;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

public class TestHelpers {

    static final Executor SAME_THREAD_EXECUTOR = Runnable::run;
    static final Scheduler SAME_THREAD_SCHEDULER = Schedulers.from(SAME_THREAD_EXECUTOR);
    static final Comparator<Trade> ID_COMPARATOR = Comparator.comparing(Trade::getTradeId);
    static final Predicate<Trade> ACCEPT_ALL = trade -> true;
    static final Predicate<Trade> ACCEPT_OPENED = trade -> !trade.getStatus().isFinal();
    static Trade setStatus(Trade trade, TradeStatus status) { return trade.toBuilder().status(status).build();};
    static UpdateEventEntry<String, Trade> createEventEntry(Trade trade, Trade old) {
        return new UpdateEventEntry(trade.getTradeId(), trade, old);
    }
}
