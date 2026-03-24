package com.butler.aeorder.controller;

import com.butler.aeorder.dto.TransactionUpdate;
import com.butler.aeorder.service.OutboxService;
import com.butler.aeorder.service.PickInstructionProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
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

    @Value("${kafka.topics.transaction-updates}")
    private String transactionUpdatesTopic;

    private final PickInstructionProcessService processService;
    private final OutboxService outboxService;

    public PickWorkflowController(PickInstructionProcessService processService,
                                   OutboxService outboxService) {
        this.processService = processService;
        this.outboxService  = outboxService;
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
    @Transactional
    @PostMapping("/transaction-update")
    public ResponseEntity<String> sendTransactionUpdate(@RequestBody TransactionUpdate update) {
        log.info("Queuing transaction update via outbox for pickId: {}, transactionId: {}, command: {}",
                update.getPickId(), update.getTransactionId(), update.getCommand());

        outboxService.save(transactionUpdatesTopic, update.getPickId(), update, "transaction_update");

        return ResponseEntity.ok("Transaction update queued for pickId: " + update.getPickId());
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
