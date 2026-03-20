package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka listener for transaction updates.
 *
 * Consumes messages from "transaction-updates-topic" published by:
 *   - POST /Order/transaction-update  (via PickWorkflowController)
 *   - Any external system publishing TransactionUpdate JSON to the topic
 *
 * On receipt, correlates the Camunda "TransactionUpdateMessage" to the waiting
 * process instance (matched by the "pickId" process variable) so it can
 * proceed past the intermediate message catch event.
 */
@Service
@Slf4j
public class TransactionUpdateListener {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Listens for transaction updates from the downstream system.
     * Multiple updates can arrive for a single pick instruction.
     * Each update correlates to the Camunda process instance waiting
     * at the TransactionUpdateMessage catch event.
     *
     * containerFactory = "jsonKafkaListenerContainerFactory" ensures
     * JSON deserialization via the configured StringDeserializer.
     */
    @KafkaListener(
            topics = "transaction-updates-topic",
            groupId = "transaction-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        TransactionUpdate transactionUpdate;
        try {
            transactionUpdate = objectMapper.readValue(payload, TransactionUpdate.class);
        } catch (Exception e) {
            log.error("Failed to deserialize TransactionUpdate from Kafka message: {}", e.getMessage());
            return;
        }

        if (transactionUpdate.getPickId() == null || transactionUpdate.getPickId().isBlank()) {
            log.error("Received transaction update with null/blank pickId — dropping");
            return;
        }

        log.info("Received transaction update | pickId: {} | transactionId: {} | command: {} | status: {}",
                transactionUpdate.getPickId(),
                transactionUpdate.getTransactionId(),
                transactionUpdate.getCommand(),
                transactionUpdate.getStatus());

        // Normalise command to uppercase (matches BPMN gateway conditions)
        String command = transactionUpdate.getCommand() != null
                ? transactionUpdate.getCommand().toUpperCase()
                : "UPDATE";

        try {
            String transactionUpdateJson = objectMapper.writeValueAsString(transactionUpdate);

            runtimeService.createMessageCorrelation("TransactionUpdateMessage")
                    .processInstanceVariableEquals("pickId", transactionUpdate.getPickId())
                    .setVariable("command", command)
                    .setVariable("txStatus", transactionUpdate.getStatus())
                    .setVariable("transactionUpdateJson", transactionUpdateJson)
                    .setVariable("transactionId", transactionUpdate.getTransactionId())
                    .setVariable("failureReason", transactionUpdate.getAdditionalInfo())
                    .correlate();

            log.info("TransactionUpdateMessage correlated successfully for pickId: {} | command: {} | status: {}",
                    transactionUpdate.getPickId(), command, transactionUpdate.getStatus());

        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance found waiting for TransactionUpdateMessage for pickId: {}. " +
                    "Transaction update dropped. transactionId: {}",
                    transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        } catch (Exception e) {
            log.error("Failed to correlate TransactionUpdateMessage for pickId: {} | error: {}",
                    transactionUpdate.getPickId(), e.getMessage(), e);
        }
    }
}
