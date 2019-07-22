package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.actions.CallAccountAction;
import com.grid.sandbox.model.actions.CallAccountBalanceAction;
import com.grid.sandbox.utils.CallAccountRemoteFilterFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.event.CacheEntryEvent;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_ACTION_TOPIC;
import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class MockEventTracker {
    private final ConcurrentHashMap<String, CallAccountAction> sentActions = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(MockEventTracker.class);
    private AtomicLong totalTime = new AtomicLong();
    private AtomicInteger actionCount = new AtomicInteger();

    @Autowired
    private Ignite ignite;

    @PostConstruct
    private void init() {
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);

        logger.info("Create query");
        ContinuousQuery<String, CallAccount> query = new ContinuousQuery<>();
        query.setLocalListener((Iterable<CacheEntryEvent<? extends String, ? extends CallAccount>> iterable) -> {
            long time = System.currentTimeMillis();
            for (CacheEntryEvent<? extends String, ? extends CallAccount> event : iterable) {
                CallAccount account = cache.get(event.getKey());
                if (account != null) {
                    account.getActions().forEach(action -> {
                        CallAccountAction sentAction = sentActions.remove(action.getActionId());
                        if (sentAction != null) {
                            actionCount.incrementAndGet();
                            totalTime.addAndGet(time - action.getTimestamp());
                        }
                    });
                } else {
                    logger.info("Update: type=" + event.getEventType() + " value=" + event.getValue().toString() + " local=false");
                }
            }
            long count = actionCount.get();
            logger.info("Unprocessed action count: " + sentActions.size() +
                    " total time=" + totalTime.get() +
                    " count=" + actionCount.get() +
                    " average time=" + (count == 0 ? 0 : totalTime.get() / count));
        });
        query.setRemoteFilterFactory(new CallAccountRemoteFilterFactory());
        query.setLocal(true);
        cache.query(query);
        CallAccount account = cache.getAsync("account-1").get();
        logger.info("get 1st async: " + account);

        ForkJoinPool.commonPool().invokeAll(generateAccountUpdates());
    }

    private Collection<? extends Callable<Void>> generateAccountUpdates() {
        IgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());
        List<Callable<Void>> actions = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            String accountId = "account-" + (i % 1000);
            BigDecimal amount = new BigDecimal(random.nextDouble() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP);
            CallAccountAction.Type type = random.nextInt() % 2 == 0 ? CallAccountAction.Type.WITHDRAW : CallAccountAction.Type.INCREASE;
            actions.add(() -> {
                CallAccountAction action = new CallAccountBalanceAction(type, accountId, amount, UUID.randomUUID().toString(), System.currentTimeMillis());
                logger.info("Sending account action " + action);
                sentActions.put(action.getActionId(), action);
                rmtMsg.sendOrdered(CALL_ACCOUNT_ACTION_TOPIC, action, 0);
                return null;
            });
        }
        return actions;
    }

}