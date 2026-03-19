package com.temporallearn.spring_temporal.controller;

import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.service.PickInstructionProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/Order")
public class PickWorkflowController {

    private static final Logger log = LoggerFactory.getLogger(PickWorkflowController.class);

    private static final String TRANSACTION_UPDATES_TOPIC = "transaction-updates-topic";

    private final PickInstructionProcessService processService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PickWorkflowController(PickInstructionProcessService processService,
                                   KafkaTemplate<String, Object> kafkaTemplate) {
        this.processService = processService;
        this.kafkaTemplate  = kafkaTemplate;
    }

    /**
     * Publishes a transaction update to Kafka.
     * The TransactionUpdateListener consumes the message and correlates the Camunda message
     * so the process can proceed past the transaction update wait point.
     *
     * Commands: UPDATE, CANCEL, COMPLETE, RETRY
     * Status:   IN_PROGRESS, COMPLETED, FAILED
     *
     * Example: POST /Order/transaction-update
     * Body: {"pickId": "Pick5", "transactionId": "TXN-002", "command": "UPDATE", ...}
     */
    @PostMapping("/transaction-update")
    public ResponseEntity<String> sendTransactionUpdate(@RequestBody TransactionUpdate update) {
        log.info("Publishing transaction update to Kafka for pickId: {}, transactionId: {}, command: {}",
                update.getPickId(), update.getTransactionId(), update.getCommand());

        kafkaTemplate.send(TRANSACTION_UPDATES_TOPIC, update.getPickId(), update);

        log.info("Transaction update published to topic '{}' for pickId: {}",
                TRANSACTION_UPDATES_TOPIC, update.getPickId());

        return ResponseEntity.ok("Transaction update published to Kafka for pickId: "
                + update.getPickId()
                + ", transactionId: " + update.getTransactionId()
                + ", command: " + update.getCommand()
                + ", status: " + update.getStatus());
    }

    /**
     * Terminate a stuck/running process instance.
     * Example: DELETE /Order/terminate/Pick5
     */
    @DeleteMapping("/terminate/{pickId}")
    public ResponseEntity<String> terminateWorkflow(@PathVariable String pickId) {
        processService.terminateProcess(pickId);
        log.info("Process terminate requested for pickId: {}", pickId);
        return ResponseEntity.ok("Process terminated for pickId: " + pickId);
    }
}
