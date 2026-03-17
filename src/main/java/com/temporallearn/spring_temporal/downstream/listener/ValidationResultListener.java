package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka listener for order validation results.
 *
 * Consumes messages from "validation-results-topic" published by:
 *   - POST /Order/validate  (via PickWorkflowController)
 *   - Any external validation system publishing ValidationResult JSON to the topic
 *
 * On receipt, correlates the Camunda "ValidationResultMessage" to the waiting
 * process instance so it can proceed past the intermediate catch event.
 */
@Service
@Slf4j
public class ValidationResultListener {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Listens for validation results from the downstream system.
     * When a result is received, it correlates the Camunda message to the
     * process instance identified by the matching "pickId" variable.
     *
     * containerFactory = "jsonKafkaListenerContainerFactory" ensures
     * JSON deserialization via the configured StringDeserializer.
     */
    @KafkaListener(
            topics = "validation-results-topic",
            groupId = "validation-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        ValidationResult result;
        try {
            result = objectMapper.readValue(payload, ValidationResult.class);
        } catch (Exception e) {
            log.error("Failed to deserialize ValidationResult from Kafka message: {}", e.getMessage());
            return;
        }

        log.info("Received validation result | orderId: {} | transactionId: {} | success: {}",
                result.getOrderId(),
                result.getTransactionId(),
                result.isSuccess());

        try {
            runtimeService.createMessageCorrelation("ValidationResultMessage")
                    .processInstanceVariableEquals("pickId", result.getOrderId())
                    .setVariable("validationSuccess", result.isSuccess())
                    .setVariable("validationResult", result.getOrderId())
                    .correlate();

            log.info("ValidationResultMessage correlated successfully for orderId: {} | success: {}",
                    result.getOrderId(), result.isSuccess());

        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance found waiting for ValidationResultMessage for orderId: {}. " +
                    "Validation result dropped.", result.getOrderId());
        } catch (Exception e) {
            log.error("Failed to correlate ValidationResultMessage for orderId: {} | error: {}",
                    result.getOrderId(), e.getMessage(), e);
        }
    }
}
