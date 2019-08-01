package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.actions.CallAccountAction;
import com.grid.sandbox.model.actions.CallAccountBalanceAction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.event.CacheEntryEvent;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
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
    private final ConcurrentSkipListSet<String> callAccountKeys = new ConcurrentSkipListSet<>();

    @Autowired
    private Ignite ignite;

    @PostConstruct
    private void init() throws InterruptedException {
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);

        logger.info("Create query");
        ContinuousQuery<String, CallAccount> query = new ContinuousQuery<>();
        query.setLocalListener((Iterable<CacheEntryEvent<? extends String, ? extends CallAccount>> iterable) -> {
            long time = System.currentTimeMillis();
            Set<String> accountKeys = new HashSet<>();
            for (CacheEntryEvent<? extends String, ? extends CallAccount> event : iterable) {
                accountKeys.add(event.getKey());
                logger.info("Update: type=" + event.getEventType() + " value=" + event.getValue().toString());
            }
            callAccountKeys.addAll(accountKeys);
            cache.getAllAsync(accountKeys).listen(new AccountListener(time));
            long count = actionCount.get();
            logger.info("Unprocessed action count: " + sentActions.size() +
                    " total time=" + totalTime.get() +
                    " count=" + actionCount.get() +
                    " average time=" + (count == 0 ? 0 : totalTime.get() / count));
        });
        query.setLocal(true);
        cache.query(query);

        ForkJoinPool.commonPool().invokeAll(generateAccountUpdates(2000));

        Thread.sleep(10000);
        logger.info("Initialization finished");
    }

    public Set<String> getCallAccountKeys() {
        return Collections.unmodifiableSet(callAccountKeys);
    }

    public Collection<? extends Callable<Void>> generateAccountUpdates(int count) {
        IgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());
        List<Callable<Void>> actions = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            String accountId = "account-" + (i % 1000);
            BigDecimal amount = new BigDecimal(random.nextDouble() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP);
            CallAccountAction.Type type = random.nextInt() % 2 == 0 ? CallAccountAction.Type.WITHDRAW : CallAccountAction.Type.INCREASE;
            CallAccountAction action = new CallAccountBalanceAction(type, accountId, amount, UUID.randomUUID().toString(), System.currentTimeMillis());
            sentActions.put(action.getActionId(), action);
            actions.add(() -> {
                logger.info("Sending account action " + action);
                rmtMsg.sendOrdered(CALL_ACCOUNT_ACTION_TOPIC, action, 0);
                return null;
            });
        }
        return actions;
    }

    private class AccountListener implements IgniteInClosure<IgniteFuture<Map<String, CallAccount>>> {
        private final long time;

        public AccountListener(long time) {
            this.time = time;
        }

        @Override
        public void apply(IgniteFuture<Map<String, CallAccount>> accountFuture) {
            Map<String, CallAccount> accounts = accountFuture.get();
            accounts.values().stream().flatMap(callAccount -> callAccount.getActions().stream()).forEach(action -> {
                CallAccountAction sentAction = sentActions.remove(action.getActionId());
                if (sentAction != null) {
                    actionCount.incrementAndGet();
                    totalTime.addAndGet(time - action.getTimestamp());
                }
            });
        }
    }

}
