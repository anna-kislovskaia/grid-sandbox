package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.HashMap;
import java.util.Map;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class CallAccountCache {
    private static final Logger logger = LoggerFactory.getLogger(CallAccountCache.class);

    @Autowired
    private Ignite ignite;

    @PostConstruct
    public void init() {
        logger.info("initialization started");
        IgniteCache<String, CallAccount> callAccounts = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);
        IgniteFuture cacheInitialization = callAccounts.putAllAsync(generateCallAccountHistory());
        cacheInitialization.get();
        logger.info("Cache is initialized");
        CallAccount account = callAccounts.get("account-1");
        logger.info("1st account " + account);
    }

    private Map<String, CallAccount> generateCallAccountHistory() {
        Map<String, CallAccount> history = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            CallAccount account = new CallAccount("account-" + i);
            history.put(account.getAccountId(),  account);
        }
        return history;
    }
}
