package com.butler.aeorder.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans.
 * Topics are auto-created on application startup if they don't already exist.
 * All topic names are prefixed with the configured tenant ID (default: "gor").
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topics.pick-instructions-request}")
    private String pickInstructionsRequestTopic;

    @Value("${kafka.topics.pick-instruction-response}")
    private String pickInstructionResponseTopic;

    @Value("${kafka.topics.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topics.pick-list-response}")
    private String pickListResponseTopic;

    @Value("${kafka.topics.transaction-updates}")
    private String transactionUpdatesTopic;

    @Value("${kafka.topics.transaction-events}")
    private String transactionEventsTopic;

    @Value("${kafka.topics.workflow-complete-events}")
    private String workflowCompleteEventsTopic;

    @Bean
    public NewTopic pickInstructionsRequestTopicBean() {
        return TopicBuilder.name(pickInstructionsRequestTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickInstructionResponseTopicBean() {
        return TopicBuilder.name(pickInstructionResponseTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickListRequestsTopicBean() {
        return TopicBuilder.name(pickListRequestsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickListResponseTopicBean() {
        return TopicBuilder.name(pickListResponseTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic transactionUpdatesTopicBean() {
        return TopicBuilder.name(transactionUpdatesTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic transactionEventsTopicBean() {
        return TopicBuilder.name(transactionEventsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic workflowCompleteEventsTopicBean() {
        return TopicBuilder.name(workflowCompleteEventsTopic).partitions(1).replicas(1).build();
    }
}
