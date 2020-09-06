package com.grid.sandbox;

import com.grid.sandbox.model.Trade;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static com.grid.sandbox.utils.CacheUtils.TRADE_CACHE;

@Configuration
public class MockServerConfig {
    private static final String LOCALHOST = "127.0.0.1";

    @Value("#{${hazelcast.join.port}}")
    private Map<String, Integer> port;

    @Value("${instance.name}")
    private String instanceName;

    @Bean
    public Config getHazelcastServerConfig() {
        Config config = new Config();
        config.setClusterName("sandbox");

        Integer instancePort = port.get(instanceName);
        config.getNetworkConfig().setPort(instancePort);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (Integer clusterPort : port.values()) {
            String address = LOCALHOST + ":" + clusterPort;
            join.getTcpIpConfig().addMember(address);
        }

        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(TRADE_CACHE);
        replicatedMapConfig.setAsyncFillup(false);
        replicatedMapConfig.getMergePolicyConfig().setPolicy(MergePolicyConfig.DEFAULT_MERGE_POLICY);
        EntryListenerConfig listenerConfig = new EntryListenerConfig();
        listenerConfig.setIncludeValue(true);
        listenerConfig.setImplementation(new ServerTradeCacheListener());
        replicatedMapConfig.addEntryListenerConfig(listenerConfig);

        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(Config config) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        return instance;
    }

    @Bean
    public ReplicatedMap<String, Trade> getTradeReplicatedMap(HazelcastInstance hazelcast) {
        return hazelcast.getReplicatedMap(TRADE_CACHE);
    }
}
