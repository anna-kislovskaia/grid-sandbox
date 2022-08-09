package com.grid.sandbox.core.service;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Service
@Log4j2
public class KeyOrderedSchedulerService {
    private Scheduler[] schedulers;
    private int threadCount = 1;

    @PostConstruct
    private void init() {
        threadCount = Runtime.getRuntime().availableProcessors();
        schedulers = new Scheduler[threadCount];
        for (int index = 0; index < threadCount; index++) {
            String name = "subscription-pool-" + index;
            ThreadFactory tf = new NamedThreadFactory(name);
            schedulers[index]= Schedulers.from(Executors.newSingleThreadExecutor(tf));
        }
        log.info("Key ordered scheduler factory is initialized as {} thread pool", threadCount);
    }

    public Scheduler getScheduler(String key) {
        int index = key == null ? 0 : Math.abs(key.hashCode()) % threadCount;
        String name = "subscription-pool-" + index;
        log.info("Scheduler for {} => {}", key, name);
        return schedulers[index];
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String name;

        private NamedThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(() -> {
                log.info("Started");
                r.run();
            }, name);
        }
    }
}
