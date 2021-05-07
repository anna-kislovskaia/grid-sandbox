package com.grid.sandbox;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.grid.sandbox.model.Trade;
import com.grid.sandbox.service.ClusterLifecycleListenerService;
import com.grid.sandbox.service.TradeUpdateFeedService;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.*;

import static com.grid.sandbox.utils.CacheUtils.TRADE_CACHE;

@EnableSwagger2
@Configuration
@Log4j2
public class MockServerConfig implements WebMvcConfigurer {
    private static final String LOCALHOST = "127.0.0.1";

    @Value("#{${hazelcast.join.port}}")
    private Map<String, Integer> port;

    @Value("${instance.name}")
    private String instanceName;

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
    public Config getHazelcastServerConfig(ClusterLifecycleListenerService lifecycleListenerService,
                                           TradeUpdateFeedService tradeUpdateFeedService)
    {
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

        config.getListenerConfigs().add(new ListenerConfig().setImplementation(lifecycleListenerService));

        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(TRADE_CACHE);
        replicatedMapConfig.setAsyncFillup(false);
        replicatedMapConfig.getMergePolicyConfig().setPolicy(MergePolicyConfig.DEFAULT_MERGE_POLICY);
        EntryListenerConfig listenerConfig = new EntryListenerConfig();
        listenerConfig.setIncludeValue(true);
        listenerConfig.setImplementation(tradeUpdateFeedService);
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

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2);
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        // reset json message converter
        List<MediaType> jsonMediaTypes = new ArrayList<>();
        jsonMediaTypes.add(new MediaType("text", "event-stream"));
        jsonMediaTypes.add(new MediaType("application", "javascript"));
        for (Iterator<HttpMessageConverter<?>> itr = converters.iterator(); itr.hasNext(); ) {
            HttpMessageConverter<?> converter = itr.next();
            if (converter instanceof MappingJackson2HttpMessageConverter) {
                jsonMediaTypes.addAll(converter.getSupportedMediaTypes());
                itr.remove();
            }
        }
        MappingJackson2HttpMessageConverter jsonConverter = new MappingJackson2HttpMessageConverter(createObjectMapper());
        jsonConverter.setSupportedMediaTypes(jsonMediaTypes);
        converters.add(jsonConverter);
        log.info("Data converters initialized");
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .enable(
                        MapperFeature.SORT_PROPERTIES_ALPHABETICALLY,
                        MapperFeature.USE_ANNOTATIONS
                ).disable(
                        MapperFeature.AUTO_DETECT_CREATORS,
                        MapperFeature.AUTO_DETECT_FIELDS,
                        MapperFeature.AUTO_DETECT_GETTERS,
                        MapperFeature.AUTO_DETECT_IS_GETTERS,
                        MapperFeature.AUTO_DETECT_SETTERS
                ).enable(
                        SerializationFeature.INDENT_OUTPUT
                ).disable(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
                ).disable(
                        DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE,
                        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
                ).registerModule(
                        new JavaTimeModule()
                ).setDefaultVisibility(
                        JsonAutoDetect.Value.construct(
                                JsonAutoDetect.Visibility.NONE,
                                JsonAutoDetect.Visibility.NONE,
                                JsonAutoDetect.Visibility.NONE,
                                JsonAutoDetect.Visibility.NONE,
                                JsonAutoDetect.Visibility.NONE
                        )
                ).setDefaultPropertyInclusion(
                        JsonInclude.Include.NON_NULL
                );
    }

}
