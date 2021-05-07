package com.grid.sandbox.service;

import com.grid.sandbox.core.service.BlotterFeedService;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.core.model.UpdateEvent;
import com.grid.sandbox.core.model.UpdateEventEntry;
import com.hazelcast.replicatedmap.ReplicatedMap;
import io.reactivex.Flowable;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Log4j2
public class TradeFeedService {
    private static final int REFRESH_INTERVAL = 10;
    private final AtomicLong lastUpdateTime = new AtomicLong();
    private final BlotterFeedService<String, Trade> feedService = new BlotterFeedService<>();

    @Autowired
    private ReplicatedMap<String, Trade> tradeCache;

    @Autowired
    private ClusterLifecycleListenerService lifecycleListenerService;

    @Autowired
    private TradeUpdateFeedService tradeUpdateFeedService;

    @PostConstruct
    private void init() {
        feedService.init();

        lifecycleListenerService.getClusterStateFeed().subscribe(lifecycleEvent -> {
            log.info("Cluster event {}", lifecycleEvent);
            if (lifecycleEvent.getType() != ClusterStateChangeEvent.Type.DISCONNECTED) {
                lastUpdateTime.set(System.currentTimeMillis());
                feedService.reset(tradeCache.values());
            }
        });

        tradeUpdateFeedService.getTradeUpdateFeed().subscribe(event -> {
            Collection<Trade> updates = event.getUpdates().stream()
                    .map(UpdateEventEntry::getValue)
                    .collect(Collectors.toList());
            feedService.update(updates);
        });

        Flowable.interval(REFRESH_INTERVAL, TimeUnit.SECONDS)
                .subscribe(i -> {
                    log.info("Loading periodic updates");
                    long lastRefresh = lastUpdateTime.getAndSet(System.currentTimeMillis());
                    List<Trade> updates = tradeCache.values().stream()
                            .filter(trade -> trade.getLastUpdateTimestamp() >= lastRefresh)
                            .collect(Collectors.toList());
                    log.info("Refresh event: {}", updates.size());
                    feedService.update(updates);
                });
    }

    public Flowable<UpdateEvent<String, Trade>> getTradeFeed(String subscriptionName) {
        return feedService.getFeed(subscriptionName);
    }

}
