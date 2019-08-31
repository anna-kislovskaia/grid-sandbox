package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.CallAccountUpdate;
import com.grid.sandbox.model.UpdateEvent;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;
import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_COUNT;

@Service
public class MockSubscriptionGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MockSubscriptionGenerator.class);
    private static final int REFRESH_INTERVAL = 30;
    private PublishSubject<CallAccountUpdate> accountUpdates = PublishSubject.create();
    private Flowable<UpdateEvent> accountSnapshots;

    @Autowired
    private MockEventTracker tracker;

    @Autowired
    private Ignite ignite;

    @PostConstruct
    private void init() {
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        logger.info("Create event publisher");
        ContinuousQuery<String, CallAccount> subscriptionQuery = new ContinuousQuery<>();
        subscriptionQuery.setLocalListener((Iterable<CacheEntryEvent<? extends String, ? extends CallAccount>> iterable) -> {
            for (CacheEntryEvent<? extends String, ? extends CallAccount> event : iterable) {
                accountUpdates.onNext(new CallAccountUpdate(event.getOldValue(), event.getValue()));
            }
        });
        subscriptionQuery.setLocal(true);
        cache.query(subscriptionQuery);

        logger.info("Loading existing keys");
        QueryCursor<Cache.Entry<String, CallAccount>> cursor = cache.query(new ScanQuery<>());
        Set<String> allAccountIds = cursor.getAll().stream().map(Cache.Entry::getKey).collect(Collectors.toSet());
        logger.info("Keys loaded: " + allAccountIds.size());
        logger.info("Cache metrics: " + cache.metrics());
        if (allAccountIds.size() != CALL_ACCOUNT_COUNT) {
            logger.error("Not all accounts loaded");
        }

        accountSnapshots = Flowable.interval(0,REFRESH_INTERVAL, TimeUnit.SECONDS, Schedulers.computation())
                .map(tick -> {
                    IgniteFuture<Map<String, CallAccount>> accountFuture = cache.getAllAsync(allAccountIds);
                    Map<String, CallAccount> data = accountFuture.get();
                    logger.info("snapshot arrived, size=" + data.size());
                    Map<String, CallAccountUpdate> snapshotEvents = new HashMap<>();
                    data.forEach( (key, value) -> snapshotEvents.put(key, new CallAccountUpdate(null, value)));
                    return new UpdateEvent(snapshotEvents, UpdateEvent.Type.SNAPSHOT);
                })
                //.startWithItem(UpdateEvent.inital)
                .share();
        allAccountIds.forEach(this::createAccountFeed);

        logger.info("Generate update events");
        ForkJoinPool.commonPool().invokeAll(tracker.generateAccountUpdates(2000));
    }

    private void createAccountFeed(String... accountIds) {
        String subscriptionId = String.join(":", accountIds);
        Set<String> accountIdSet = new HashSet<>(Arrays.asList(accountIds));
        Logger subscriptionLogger = LoggerFactory.getLogger(subscriptionId);
        // load snapshot
        Flowable<UpdateEvent> snapshotFlowable = accountSnapshots
                .map(snapshot -> {
                    if (snapshot.getType() == UpdateEvent.Type.INITIAL) {
                        return snapshot;
                    }
                    Map<String, CallAccountUpdate> accounts = accountIdSet.stream()
                            .map(id -> snapshot.getUpdates().get(id))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toMap(CallAccountUpdate::getAccountId, update -> update));
                    return new UpdateEvent(accounts, UpdateEvent.Type.SNAPSHOT);
                });

        // filter updates
        ConnectableFlowable<UpdateEvent> updatesFlowable = accountUpdates
                .subscribeOn(Schedulers.computation())
                .toFlowable(BackpressureStrategy.BUFFER)
                .filter(event -> accountIdSet.contains(event.getAccountId()))
                .map(event -> new UpdateEvent(Collections.singletonMap(event.getAccountId(), event), UpdateEvent.Type.INCREMENTAL))
                .replay(REFRESH_INTERVAL * 2, TimeUnit.SECONDS);
                //.startWithItem(UpdateEvent.inital);

        // combine snapshot and updates
        Map<String, CallAccountUpdate> reported = new ConcurrentHashMap<>();
        AtomicBoolean updatesConnected = new AtomicBoolean();
        Flowable<UpdateEvent> merged = Flowable.merge(snapshotFlowable, updatesFlowable)
                .map(event -> {
                    subscriptionLogger.info("Received " + event);
                    Map<String, CallAccountUpdate> updates = merge(reported, event);
                    if (event.getType() == UpdateEvent.Type.SNAPSHOT) {
                        subscriptionLogger.info("report snapshot");
                        if (updatesConnected.compareAndSet(false, true)) {
                            subscriptionLogger.info("replay updates");
                            updatesFlowable.connect();
                        }
                        return new UpdateEvent(new HashMap<>(reported), UpdateEvent.Type.SNAPSHOT);
                    } else if (!updates.isEmpty()){
                        subscriptionLogger.info("report diff");
                        return new UpdateEvent(updates, UpdateEvent.Type.INCREMENTAL);
                    }
                    return UpdateEvent.inital;
                })
                .filter(event -> !event.getUpdates().isEmpty());

        // test
        merged.subscribeOn(Schedulers.computation()).subscribe(event -> {
            subscriptionLogger.info("Emitted " + event);
        });
    }

    private static Map<String, CallAccountUpdate> merge(Map<String, CallAccountUpdate> existing, UpdateEvent event) {
        Map<String, CallAccountUpdate> diff = new HashMap<>();
        event.getUpdates().values().forEach(update -> {
            CallAccountUpdate old = existing.get(update.getAccountId());
            CallAccountUpdate merged = update.merge(old);
            existing.put(merged.getAccountId(), merged);
            if (old != merged) {
                diff.put(merged.getAccountId(), merged);
            }
        });
        // cleanup map
        if (event.getType() == UpdateEvent.Type.SNAPSHOT) {
            existing.keySet().retainAll(event.getUpdates().keySet());
        }
        return diff;
    }
}
