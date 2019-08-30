package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.CallAccountUpdate;
import com.grid.sandbox.model.UpdateEvent;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;
import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_COUNT;

@Service
public class MockSubscriptionGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MockSubscriptionGenerator.class);
    private PublishSubject<CallAccountUpdate> accountUpdates = PublishSubject.create();

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
        List<Cache.Entry<String, CallAccount>> allAccounts = cursor.getAll();
        logger.info("Keys loaded: " + allAccounts.size());
        logger.info("Cache metrics: " + cache.metrics());
        if (allAccounts.size() != CALL_ACCOUNT_COUNT) {
            logger.error("Not all accounts loaded");
        }
        allAccounts.stream().map(Cache.Entry::getKey).forEach(this::createAccountFeed);

        logger.info("Generate update events");
        ForkJoinPool.commonPool().invokeAll(tracker.generateAccountUpdates(2000));
    }

    private void createAccountFeed(String... accountIds) {
        String subscriptionId = String.join(":", accountIds);
        Set<String> accountIdSet = new HashSet<>(Arrays.asList(accountIds));
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        Logger subscriptionLogger = LoggerFactory.getLogger(subscriptionId);
        // load snapshot
        Flowable<UpdateEvent> snapshotFlowable = Flowable.interval(0,30, TimeUnit.SECONDS, Schedulers.computation())
                .map(tick -> {
                    IgniteFuture<Map<String, CallAccount>> accountFuture = cache.getAllAsync(accountIdSet);
                    Map<String, CallAccount> data = accountFuture.get();
                    subscriptionLogger.info("snapshot arrived, size=" + data.size());
                    Map<String, CallAccountUpdate> snapshotEvents = new HashMap<>();
                    data.forEach( (key, value) -> snapshotEvents.put(key, new CallAccountUpdate(null, value)));
                    return new UpdateEvent(snapshotEvents, UpdateEvent.Type.SNAPSHOT);
                })
                .startWithItem(UpdateEvent.inital);

        // filter updates
        Flowable<UpdateEvent> updatesFlowable = accountUpdates
                .subscribeOn(Schedulers.computation())
                .toFlowable(BackpressureStrategy.BUFFER)
                .filter(event -> accountIdSet.contains(event.getAccountId()))
                .map(event -> new UpdateEvent(Collections.singletonMap(event.getAccountId(), event), UpdateEvent.Type.INCREMENTAL))
                .startWithItem(UpdateEvent.inital);

        // combine snapshot and updates
        AtomicInteger snapshotHash = new AtomicInteger(0);
        Map<String, CallAccountUpdate> reported = new ConcurrentHashMap<>();
        Function<Object[], UpdateEvent> combiner = results -> {
            UpdateEvent snapshotEvent = (UpdateEvent)results[0];
            UpdateEvent updateEvent = (UpdateEvent)results[1];
            if (snapshotEvent.getType() == UpdateEvent.Type.INITIAL) {
                subscriptionLogger.info("updates received, no snapshot");
                merge(reported, updateEvent.getUpdates());
                return UpdateEvent.inital;
            } else {
                boolean snapshotUpdated = snapshotHash.getAndSet(snapshotEvent.hashCode()) != snapshotEvent.hashCode();
                if (snapshotUpdated) {
                    // report full snapshot
                    subscriptionLogger.info("report snapshot");
                    merge(reported, snapshotEvent.getUpdates());
                    return new UpdateEvent(new HashMap<>(reported), UpdateEvent.Type.SNAPSHOT);
                } else if (!updateEvent.getUpdates().isEmpty()) {
                    // report diff only
                    subscriptionLogger.info("report diff");
                    Map<String, CallAccountUpdate> updates = merge(reported, updateEvent.getUpdates());
                    return new UpdateEvent(updates, UpdateEvent.Type.INCREMENTAL);
                }
                return UpdateEvent.inital;
            }
        };
        Flowable<UpdateEvent> merged = Flowable.combineLatest(combiner, snapshotFlowable, updatesFlowable)
                .filter(event -> !event.getUpdates().isEmpty());

        // test
        merged.subscribeOn(Schedulers.computation()).subscribe(event -> {
            subscriptionLogger.info("Event " + event);
        });
    }

    private static Map<String, CallAccountUpdate> merge(Map<String, CallAccountUpdate> existing, Map<String, CallAccountUpdate> events) {
        Map<String, CallAccountUpdate> diff = new HashMap<>();
        events.values().forEach(update -> {
            CallAccountUpdate old = existing.get(update.getAccountId());
            CallAccountUpdate merged = update.merge(old);
            existing.put(merged.getAccountId(), merged);
            if (old != merged) {
                diff.put(merged.getAccountId(), merged);
            }
        });
        return diff;
    }
}
