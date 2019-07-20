package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.actions.CallAccountAction;
import com.grid.sandbox.utils.CallAccountRemoteFilterFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.cache.event.CacheEntryEvent;
import java.util.concurrent.ConcurrentHashMap;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class MockEventTracker {
    private final ConcurrentHashMap<String, CallAccountAction> sentActions = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(MockEventTracker.class);

    @Autowired
    private Ignite ignite;

    @PostConstruct
    private void init() {
        IgniteCache<String, CallAccount> cache = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);

        logger.info("Create query");
        ContinuousQuery<String, CallAccount> query = new ContinuousQuery<>();
        query.setLocalListener((Iterable<CacheEntryEvent<? extends String, ? extends CallAccount>> iterable) -> {
            for (CacheEntryEvent<? extends String, ? extends CallAccount> event : iterable) {
                CallAccount account = cache.get(event.getKey());
                logger.info("Update: type=" + event.getEventType() + " value=" + event.getValue().toString() + " local=" + (account != null));
            }
        });
        query.setRemoteFilterFactory(new CallAccountRemoteFilterFactory());
        query.setLocal(true);
        cache.query(query);
        CallAccount account = cache.getAsync("account-1").get();
        logger.info("get 1st async: " + account);

    }

}
