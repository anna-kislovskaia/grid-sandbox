package com.grid.sandbox;

import com.grid.sandbox.model.Trade;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static com.grid.sandbox.utils.CacheUtils.TRADE_CACHE;

@EnableSwagger2
@Configuration
public class MockAutoClientConfig {

    private static final String LOCALHOST = "127.0.0.1";

    @Value("#{${hazelcast.join.port}}")
    private Map<String, Integer> port;

    @Value("${hazelcast.client.port}")
    private Integer clientPort;

    @Bean
    public ClientConfig hazelcastClientConfig() {
        ClientConfig config = new ClientConfig();
        config.setClusterName("sandbox");

        // network
        ClientNetworkConfig networkConfig = config.getNetworkConfig()
                .addOutboundPort(clientPort)
                .setSmartRouting(false)
                .setRedoOperation(true)
                .setConnectionTimeout(60000);
        for (Integer clusterPort : port.values()) {
            String address = LOCALHOST + ":" + clusterPort;
            networkConfig.getAddresses().add(address);
        }

        NearCacheConfig tradeCacheConfig = new NearCacheConfig(TRADE_CACHE);
        tradeCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
        config.addNearCacheConfig(tradeCacheConfig);

        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(ClientConfig config) {
        HazelcastInstance instance = HazelcastClient.newHazelcastClient(config);
        return instance;
    }

    @Bean
    public ReplicatedMap<String, Trade> getTradeReplicatedMap(HazelcastInstance hazelcast) {
        return hazelcast.getReplicatedMap(TRADE_CACHE);
    }

    @Bean(name = "tradeComparators")
    public Map<String, Comparator<Trade>> getTradePropertyComparators() {
        Map<String, Comparator<Trade>> tradeComparators = new HashMap<>();
        tradeComparators.put("lastUpdateTimestamp",
                (trade1, trade2) -> Long.compare(trade1.getLastUpdateTimestamp(), trade2.getLastUpdateTimestamp()));
        tradeComparators.put("client",
                (trade1, trade2) -> trade1.getClient().compareTo(trade2.getClient()));
        tradeComparators.put("balance",
                (trade1, trade2) -> trade1.getBalance().compareTo(trade2.getBalance()));
        tradeComparators.put("status",
                (trade1, trade2) -> trade1.getStatus().compareTo(trade2.getStatus()));
        return tradeComparators;
    }

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2);
    }

}
