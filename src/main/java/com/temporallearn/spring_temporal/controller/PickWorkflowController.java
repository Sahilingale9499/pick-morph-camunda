package com.temporallearn.spring_temporal.controller;

import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.ValidationResult;
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
    private static final String VALIDATION_RESULTS_TOPIC  = "validation-results-topic";

    private final PickInstructionProcessService processService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PickWorkflowController(PickInstructionProcessService processService,
                                   KafkaTemplate<String, Object> kafkaTemplate) {
        this.processService = processService;
        this.kafkaTemplate  = kafkaTemplate;
    }

    /**
     * Start a new Camunda process instance for the given PickInstruction.
     * Idempotent: duplicate starts for the same pickId are silently ignored.
     *
     * Example: POST /Order/pick_instruction
     * Body: {"pickId": "Pick5", "item": "SKU-001", ...}
     */
    @PostMapping("/pick_instruction")
    public ResponseEntity<String> startPickInstruction(@RequestBody PickInstruction pickInstruction) {
        processService.startProcess(pickInstruction);
        log.info("Pick instruction process started for pickId: {}", pickInstruction.getPickId());
        return ResponseEntity.ok("Pick instruction process started for pickId: "
                + pickInstruction.getPickId());
    }

    /**
     * Publishes a validation result to Kafka.
     * The ValidationResultListener consumes the message and correlates the Camunda message
     * so the process can proceed past the validation wait point.
     *
     * Example: POST /Order/validate
     * Body: {"orderId": "Pick5", "transactionId": "TXN-001", "success": true}
     */
    @PostMapping("/validate")
    public ResponseEntity<String> sendValidationResult(@RequestBody ValidationResult result) {
        log.info("Publishing validation result to Kafka for orderId: {}, transactionId: {}",
                result.getOrderId(), result.getTransactionId());

        kafkaTemplate.send(VALIDATION_RESULTS_TOPIC, result.getOrderId(), result);

        log.info("Validation result published to topic '{}' for orderId: {}",
                VALIDATION_RESULTS_TOPIC, result.getOrderId());

        return ResponseEntity.ok("Validation result published to Kafka for orderId: "
                + result.getOrderId() + ", transactionId: " + result.getTransactionId());
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
