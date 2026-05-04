package com.daniellemustillo.datalake_demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * KafkaTopicConfig
 */ 
@Configuration
@Profile("!test")
public class KafkaTopicConfig {

    @Value(value="${spring.kafka.ad-click-topic}")
    private String adClicks;

    @Value(value="${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Bean
    KafkaAdmin KafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    NewTopic adclicktopic() {
        return new NewTopic(adClicks, 1, (short) 1);
    }
}
