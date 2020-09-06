package com.grid.sandbox;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.model.TradeStatus;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_COUNT;

@Service
public class TradeCacheUpdater {
    private static final Logger logger = LoggerFactory.getLogger(TradeCacheUpdater.class);
    private static final String[] CLIENTS = new String[]{"Client 1", "Client 2", "Client 3"};

    @Value("${instance.name}")
    private String instanceName;

    @Autowired
    private ReplicatedMap<String, Trade> tradeCache;

    private final AtomicInteger tradeIdGenerator = new AtomicInteger();

    @PostConstruct
    public void init() {
        logger.info("initialization started: {}: {}", instanceName, tradeCache.size());
        generateTradeHistory();
        logger.info("Cache is initialized: {}", tradeCache.size());
    }

    private void generateTradeHistory() {
        Random random = new Random();
        TradeStatus[] statuses = TradeStatus.values();
        for (int i = 0; i < CALL_ACCOUNT_COUNT; i++) {
            Trade trade = Trade.toBuilder()
                    .tradeId(generateNextTradeId())
                    .client(getRandomValue(CLIENTS, random.nextInt()))
                    .status(getRandomValue(statuses, random.nextInt()))
                    .lastUpdateTimestamp(System.currentTimeMillis())
                    .balance(BigDecimal.valueOf(random.nextDouble() * 10000))
                    .build();
            tradeCache.put(trade.getTradeId(),  trade);
        }
    }

    private <T> T getRandomValue(T[] array, int index) {
        return array[Math.abs(index) % array.length];
    }

    private String generateNextTradeId() {
        return instanceName + "." + tradeIdGenerator.incrementAndGet();
    }
}
