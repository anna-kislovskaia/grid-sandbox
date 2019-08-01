package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.CallAccountUpdate;
import com.grid.sandbox.model.UpdateEvent;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.lang.IgniteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.event.CacheEntryEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class MockSubscriptionGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MockSubscriptionGenerator.class);
    private PublishSubject<CallAccountUpdate> accountUpdates = PublishSubject.create();

    @Autowired
    private MockEventTracker tracker;

    @Autowired
    private Ignite ignite;


    private void createAccountFeed(String... accountIds) {
        String subscriptionId = String.join(":", accountIds);
        Set<String> accountIdSet = new HashSet<>(Arrays.asList(accountIds));
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        // load snapshot
        Flowable<UpdateEvent> snapshotFlowable = Flowable.interval(0,30, TimeUnit.SECONDS, Schedulers.computation())
                .map(tick -> {
                    IgniteFuture<Map<String, CallAccount>> accountFuture = cache.getAllAsync(accountIdSet);
                    Map<String, CallAccount> data = accountFuture.get();
                    logger.info(subscriptionId + ": snapshot arrived, size=" + data.size());
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
        logger.info(subscriptionId + ": update feed created");

        // combine snapshot and updates
        AtomicInteger snapshotHash = new AtomicInteger(0);
        Map<String, CallAccountUpdate> reported = new ConcurrentHashMap<>();
        Function<Object[], UpdateEvent> combiner = results -> {
            UpdateEvent snapshotEvent = (UpdateEvent)results[0];
            UpdateEvent updateEvent = (UpdateEvent)results[1];
            if (snapshotEvent.getType() == UpdateEvent.Type.INITIAL) {
                logger.info(subscriptionId + ": updates received, no snapshot");
                merge(reported, updateEvent.getUpdates());
                return UpdateEvent.inital;
            } else {
                boolean snapshotUpdated = snapshotHash.getAndSet(snapshotEvent.hashCode()) != snapshotEvent.hashCode();
                if (snapshotUpdated) {
                    // report full snapshot
                    logger.info(subscriptionId + ": report snapshot");
                    merge(reported, snapshotEvent.getUpdates());
                    return new UpdateEvent(new HashMap<>(reported), UpdateEvent.Type.SNAPSHOT);
                } else if (!updateEvent.getUpdates().isEmpty()) {
                    // report diff only
                    logger.info(subscriptionId + ": report diff");
                    Map<String, CallAccountUpdate> updates = merge(reported, updateEvent.getUpdates());
                    return new UpdateEvent(updates, UpdateEvent.Type.INCREMENTAL);
                }
                return UpdateEvent.inital;
            }
        };
        Flowable<UpdateEvent> merged = Flowable.combineLatest(combiner, snapshotFlowable, updatesFlowable)
                .filter(event -> !event.getUpdates().isEmpty());
        logger.info(subscriptionId + ": merged feed created");

        // test
        merged.subscribeOn(Schedulers.computation()).subscribe(event -> {
            logger.info(subscriptionId + ": arrived " + event);
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

        tracker.getCallAccountKeys().forEach(this::createAccountFeed);

        logger.info("Generate update events");
        ForkJoinPool.commonPool().invokeAll(tracker.generateAccountUpdates(2000));
    }
}
