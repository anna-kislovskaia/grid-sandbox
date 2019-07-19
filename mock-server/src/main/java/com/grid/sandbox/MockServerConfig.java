package com.grid.sandbox;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MockServerConfig {

    @Bean
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName("ignite-cluster-node");
        CacheConfiguration ccfg1 = new CacheConfiguration("PersonCache");
        ccfg1.setIndexedTypes(Long.class, Long.class);
        CacheConfiguration ccfg2 = new CacheConfiguration("ContactCache");
        ccfg2.setIndexedTypes(Long.class, Long.class);
        cfg.setCacheConfiguration(ccfg1, ccfg2);
        return Ignition.start(cfg);
    }
}
