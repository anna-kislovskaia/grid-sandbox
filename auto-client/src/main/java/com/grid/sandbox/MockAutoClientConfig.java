package com.grid.sandbox;

import com.grid.sandbox.model.Trade;
import com.grid.sandbox.service.ClusterLifecycleListenerService;
import com.grid.sandbox.service.TradeUpdateFeedService;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.ListenerConfig;
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
    public ClusterLifecycleListenerService lifecycleListenerService() {
        return new ClusterLifecycleListenerService();
    }

    @Bean
    public TradeUpdateFeedService tradeUpdateFeedService() {
        TradeUpdateFeedService service = new TradeUpdateFeedService();
        service.init();
        return service;
    }

    @Bean
    public ClientConfig hazelcastClientConfig(ClusterLifecycleListenerService listenerService) {
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

        // listeners
        config.getListenerConfigs().add(new ListenerConfig().setImplementation(listenerService));

        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(ClientConfig config, ClusterLifecycleListenerService lifecycleListenerService) {
        HazelcastInstance instance = HazelcastClient.newHazelcastClient(config);
        instance.getLifecycleService().addLifecycleListener(lifecycleListenerService);
        return instance;
    }

    @Bean
    public ReplicatedMap<String, Trade> getTradeReplicatedMap(HazelcastInstance hazelcast,
                                                              TradeUpdateFeedService tradeUpdateFeedService)
    {
        ReplicatedMap<String, Trade>  cache = hazelcast.getReplicatedMap(TRADE_CACHE);
        cache.addEntryListener(tradeUpdateFeedService);
        return cache;
    }

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2);
    }

}
