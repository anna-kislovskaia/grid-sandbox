package com.grid.sandbox;

import com.grid.sandbox.model.CallAccount;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.grid.sandbox.utils.CacheUtils.CALL_ACCOUNT_CACHE;

@Configuration
public class MockServerConfig {

    @Bean
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("mock-server-node");
        CacheConfiguration callAccounts = new CacheConfiguration<Long, CallAccount>(CALL_ACCOUNT_CACHE)
                        .setIndexedTypes(Long.class, CallAccount.class);
        cfg.setCacheConfiguration(callAccounts);
        return Ignition.start(cfg);
    }
}
