package com.temporallearn.spring_temporal.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans.
 * Topics are auto-created on application startup if they don't already exist.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic transactionUpdatesTopic() {
        return TopicBuilder.name("transaction-updates-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic validationResultsTopic() {
        return TopicBuilder.name("validation-results-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
