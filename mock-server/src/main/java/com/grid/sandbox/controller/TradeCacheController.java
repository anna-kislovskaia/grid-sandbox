package com.grid.sandbox.controller;

import com.grid.sandbox.service.TradeCacheUpdater;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Controller
@RequestMapping("/trades/cache")
public class TradeCacheController {

    @Autowired
    private TradeCacheUpdater cacheUpdater;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private AtomicReference<ScheduledFuture<?>> currentTask = new AtomicReference<>();

    @PutMapping(path = "/clear")
    public void clear() {
        cacheUpdater.clearCache();
    }

    @PutMapping(path = "/generate")
    public void generateNewTrades(@RequestParam int count) {
        cacheUpdater.generateTradeHistory(count);
    }

    @PutMapping(path = "/start")
    public void startTradeUpdates(@RequestParam int count, @RequestParam int period) {
        stopTradeUpdates();
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(
                () -> {
                    for(int i = 0; i < count; i++) {
                        cacheUpdater.updateRandomTrade();
                    };
                }, 0, period, TimeUnit.MILLISECONDS);
        if (!currentTask.compareAndSet(null, task)) {
            task.cancel(true);
        }
    }

    @PutMapping(path = "/stop")
    public void stopTradeUpdates() {
        ScheduledFuture<?> task = currentTask.get();
        if (task != null && currentTask.compareAndSet(task, null)) {
            task.cancel(true);
        }
    }
}
