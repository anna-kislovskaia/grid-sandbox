package com.grid.sandbox.core.service;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

@Service
@Log4j2
public class KeyOrderedSchedulerService {
    private ConcurrentMap<Integer, Scheduler> schedulers = new ConcurrentHashMap<>();
    private int threadCount = 1;

    @PostConstruct
    private void init() {
        threadCount = Runtime.getRuntime().availableProcessors();
        log.info("Key ordered scheduler factory is initialized as {} thread pool", threadCount);
    }

    public Scheduler getScheduler(String key) {
        int index = key == null ? 0 : Math.abs(key.hashCode()) % threadCount;
        log.info("Scheduler for {} => {}", key, index);
        return schedulers.computeIfAbsent(index, k -> Schedulers.from(Executors.newSingleThreadExecutor()));
    }
}
