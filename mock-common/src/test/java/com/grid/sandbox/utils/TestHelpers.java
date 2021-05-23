package com.grid.sandbox.utils;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.TradeStatus;
import com.grid.sandbox.core.model.UpdateEventEntry;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHelpers {

    public static final Executor SAME_THREAD_EXECUTOR = Runnable::run;
    public static final Scheduler SAME_THREAD_SCHEDULER = Schedulers.from(SAME_THREAD_EXECUTOR);
    public static final Comparator<Trade> ID_COMPARATOR = Comparator.comparing(Trade::getTradeId);
    public static Trade setStatus(Trade trade, TradeStatus status) { return trade.toBuilder().status(status).build();};
    public static UpdateEventEntry<String, Trade> createEventEntry(Trade trade, Trade old) {
        return new UpdateEventEntry<>(trade, old);
    }

    public static List<Trade> generateTrades() {
        ArrayList<Trade> testTrades = new ArrayList<>();

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
        return testTrades;
    }

    private static String[] CLIENTS = new String[]{"client 1", "client 2", "client 3", "client 4"};
    private static Random random = new Random();
    private static AtomicInteger idGenerator = new AtomicInteger();

    public static Trade generateNewTrade() {
        Trade.Builder builder = Trade.builder()
                .tradeId("trade-" + idGenerator.incrementAndGet())
                .client(getRandomValue(CLIENTS, random.nextInt()));
        return updateTrade(builder);
    }

    public static Trade updateTrade(Trade.Builder builder) {
        return builder
                .status(getRandomValue(TradeStatus.values(), random.nextInt()))
                .lastUpdateTimestamp(System.currentTimeMillis())
                .balance(BigDecimal.valueOf(random.nextDouble() * 10000).setScale(2, RoundingMode.HALF_UP))
                .build();
    }

    private static <T> T getRandomValue(T[] array, int index) {
        return array[Math.abs(index) % array.length];
    }

}
