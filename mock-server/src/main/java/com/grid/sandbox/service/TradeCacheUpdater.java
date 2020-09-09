package com.grid.sandbox.service;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.TradeStatus;
import com.hazelcast.replicatedmap.ReplicatedMap;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_COUNT;

@Log4j2
@Service
public class TradeCacheUpdater {
    private static final String[] CLIENTS = new String[]{"Client 1", "Client 2", "Client 3"};
    private static final TradeStatus[] STATUSES = TradeStatus.values();
    private static final Random random = new Random();

    @Value("${instance.name}")
    private String instanceName;

    @Autowired
    private ReplicatedMap<String, Trade> tradeCache;

    private final AtomicInteger tradeIdGenerator = new AtomicInteger();

    @PostConstruct
    public void init() {
        log.info("initialization started: {}: {}", instanceName, tradeCache.size());
        generateTradeHistory(CALL_ACCOUNT_COUNT);
        log.info("Cache is initialized: {}", tradeCache.size());
    }

    public void generateTradeHistory(int size) {
        for (int i = 0; i < size; i++) {
            Trade trade = generateNewTrade();
            tradeCache.put(trade.getTradeId(),  trade);
        }
    }


    public void clearCache() {
        log.info("Clearing cache of size {}", tradeCache.size());
        tradeCache.clear();
        tradeIdGenerator.set(0);
        log.info("Cleared: {}", tradeCache.size());
    }

    public void updateRandomTrade() {
        Trade trade = null;
        while (trade == null) {
            int lastId = tradeIdGenerator.get();
            int sequence = random.nextInt() % lastId;
            String tradeId = instanceName + "." + sequence;
            trade = tradeCache.get(tradeId);
        }
        Trade updated = updateTrade(trade.toBuilder());
        tradeCache.put(updated.getTradeId(), updated);
        log.info("Updated {}", updated);
    }

    private Trade generateNewTrade() {
        Trade.Builder builder = Trade.builder()
                .tradeId(generateNextTradeId())
                .client(getRandomValue(CLIENTS, random.nextInt()));
        return updateTrade(builder);
    }

    private static Trade updateTrade(Trade.Builder builder) {
        return builder
                .status(getRandomValue(STATUSES, random.nextInt()))
                .lastUpdateTimestamp(System.currentTimeMillis())
                .balance(BigDecimal.valueOf(random.nextDouble() * 10000).setScale(2, RoundingMode.HALF_UP))
                .build();
    }

    private static <T> T getRandomValue(T[] array, int index) {
        return array[Math.abs(index) % array.length];
    }

    private String generateNextTradeId() {
        return instanceName + "." + tradeIdGenerator.incrementAndGet();
    }
}
