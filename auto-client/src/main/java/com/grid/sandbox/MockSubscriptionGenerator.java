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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.event.CacheEntryEvent;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class MockSubscriptionGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MockSubscriptionGenerator.class);
    private PublishSubject<Map<String, CallAccountUpdate>> accountUpdates = PublishSubject.create();

    @Autowired
    private MockEventTracker tracker;

    @Autowired
    private Ignite ignite;


    private void createAccountFeed(String... accountIds) {
        String subscriptionId = String.join(":", accountIds);
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        // load snapshot
        BehaviorSubject<UpdateEvent> snapshot = BehaviorSubject.create();
        Flowable<UpdateEvent> snapshotFlowable = snapshot.toFlowable(BackpressureStrategy.LATEST)
                .startWithItem(UpdateEvent.inital());
        Set<String> accountIdSet = new HashSet<>(Arrays.asList(accountIds));
        cache.getAllAsync(accountIdSet).listen(accountFuture -> {
            Map<String, CallAccount> data = accountFuture.get();
            logger.info(subscriptionId + ": snapshot arrived, size=" + data.size());
            Map<String, CallAccountUpdate> snapshotEvents = new HashMap<>();
            data.forEach( (key, value) -> snapshotEvents.put(key, new CallAccountUpdate(null, value)));
            snapshot.onNext(new UpdateEvent(snapshotEvents, UpdateEvent.Type.SNAPSHOT));
            logger.info(subscriptionId + ": snapshot feed complete");
        });

        // filter updates
        Flowable<UpdateEvent> updatesFlowable = accountUpdates
                .toFlowable(BackpressureStrategy.BUFFER)
                .map(events -> {
                    Map<String, CallAccountUpdate> copy = new HashMap<>(events);
                    copy.keySet().retainAll(accountIdSet);
                    return new UpdateEvent(copy, UpdateEvent.Type.INCREMENTAL);
                }).startWithItem(UpdateEvent.inital());
        logger.info(subscriptionId + ": update feed created");

        // combine snapshot and updates
        AtomicReference<UpdateEvent> latestSnapshot = new AtomicReference<>();
        Function<Object[], UpdateEvent> combiner = results -> {
            UpdateEvent snapshotEvent = (UpdateEvent)results[0];
            UpdateEvent updateEvent = (UpdateEvent)results[1];
            if (snapshotEvent.getType() == UpdateEvent.Type.INITIAL) {
                UpdateEvent merged = mergeSnapshots(latestSnapshot.get(), updateEvent.getUpdates());
                latestSnapshot.set(merged);
                return UpdateEvent.inital();
            } else {
                UpdateEvent latest = latestSnapshot.get();
                if (latest != snapshotEvent && latestSnapshot.compareAndSet(latest, snapshotEvent)) {
                    // report merged snapshot
                    return mergeSnapshots(latest, snapshotEvent.getUpdates());
                } else {
                    // report updates only
                    Map<String, CallAccountUpdate> updates = new HashMap<>();
                    updateEvent.getUpdates().forEach((key, value) -> {
                        CallAccountUpdate existing = latest.getUpdates().get(key);
                        if (existing == null || existing.getVersion() < value.getVersion()) {
                            updates.put(key, value);
                        }
                    });
                    return new UpdateEvent(updates, UpdateEvent.Type.INCREMENTAL);
                }
            }
        };
        Flowable<UpdateEvent> merged = Flowable.combineLatest(combiner, snapshotFlowable, updatesFlowable)
                .filter(event -> !event.getUpdates().isEmpty());
        logger.info(subscriptionId + ": merged feed created");

        // test
        merged.subscribeOn(Schedulers.computation()).subscribe(event -> {
            logger.info(subscriptionId + ": event arrived" + event);
        });
    }

    private static UpdateEvent mergeSnapshots(UpdateEvent existing, Map<String, CallAccountUpdate> events) {
        Map<String, CallAccountUpdate> mergedEvents = new HashMap<>();
        if (existing != null) {
            mergedEvents.putAll(existing.getUpdates());
        }
        events.values().forEach(update -> {
            CallAccountUpdate old = mergedEvents.get(update.getAccountId());
            CallAccountUpdate merged = update.merge(old);
            mergedEvents.put(merged.getAccountId(), merged);
        });
        return new UpdateEvent(mergedEvents, UpdateEvent.Type.SNAPSHOT);
    }

    @PostConstruct
    private void init() {
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        logger.info("Create event publisher");
        ContinuousQuery<String, CallAccount> subscriptionQuery = new ContinuousQuery<>();
        subscriptionQuery.setLocalListener((Iterable<CacheEntryEvent<? extends String, ? extends CallAccount>> iterable) -> {
            Map<String, CallAccountUpdate> events = new HashMap<>();
            for (CacheEntryEvent<? extends String, ? extends CallAccount> event : iterable) {
                CallAccountUpdate old = events.get(event.getKey());
                CallAccountUpdate received = new CallAccountUpdate(event.getOldValue(), event.getValue()).merge(old);
                events.put(event.getKey(), received);
            }
            accountUpdates.onNext(events);
        });
        subscriptionQuery.setLocal(true);
        cache.query(subscriptionQuery);

        tracker.getCallAccountKeys().forEach(this::createAccountFeed);

        logger.info("Generate update events");
        ForkJoinPool.commonPool().invokeAll(tracker.generateAccountUpdates(2000));
    }
}
