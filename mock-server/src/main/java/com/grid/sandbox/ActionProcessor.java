package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import com.grid.sandbox.model.actions.CallAccountAction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_ACTION_TOPIC;
import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Service
public class ActionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ActionProcessor.class);

    @Autowired
    private Ignite ignite;

    @PostConstruct
    private void init() {
        logger.info("Starting action processor");
        IgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());
        IgniteCache<String, CallAccount> callAccounts = ignite.getOrCreateCache(CALL_ACCOUNT_CACHE);

        // Add listener for ordered messages on all remote nodes.
        rmtMsg.localListen(CALL_ACCOUNT_ACTION_TOPIC, (nodeId, msg) -> {
            logger.info("Message arrived: " + msg);

            if (msg instanceof CallAccountAction) {
                CallAccountAction action = (CallAccountAction)msg;
                CallAccount account = callAccounts.get(action.getAccountId());
                if (account != null) {
                    CallAccount updated = account.apply(action);
                    callAccounts.putAsync(action.getAccountId(), updated);
                }
            }
            return true; // Return true to continue listening.
        });
    }
}
