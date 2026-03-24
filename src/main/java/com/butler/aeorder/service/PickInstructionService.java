package com.butler.aeorder.service;

import com.butler.aeorder.dto.AePickListRequest;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.dto.TransactionUpdate;
import com.butler.aeorder.dto.UpdatePickInstructionDto;
import com.butler.aeorder.dto.UpdatePickInstructionResult;
import com.butler.aeorder.grpc.ButlerCoreGrpcClient;
import com.butler.aeorder.model.TransactionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Orchestration service used by Camunda JavaDelegate classes.
 *
 * Coordinates AE order building (AeOrderBuilderService), PostgreSQL persistence
 * (AeOrderPersistenceService), gRPC calls to Butler Core, and Kafka publishing.
 */
@Service
@Slf4j
public class PickInstructionService {

    private final AeOrderBuilderService aeOrderBuilderService;
    private final AeOrderPersistenceService aeOrderPersistenceService;
    private final ButlerCoreGrpcClient butlerCoreGrpcClient;
    private final OutboxService outboxService;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${kafka.topics.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topics.pick-instruction-response}")
    private String pickInstructionResponseTopic;

    @Value("${kafka.topics.transaction-events}")
    private String transactionEventsTopic;

    @Value("${kafka.topics.workflow-complete-events}")
    private String workflowCompleteEventsTopic;

    private static final int UPDATE_MAX_RETRIES = 2;
    private static final long RETRY_BASE_DELAY_MS = 500L;
    private static final long RETRY_MAX_DELAY_MS = 2000L;

    public PickInstructionService(AeOrderBuilderService aeOrderBuilderService,
                                  AeOrderPersistenceService aeOrderPersistenceService,
                                  ButlerCoreGrpcClient butlerCoreGrpcClient,
                                  OutboxService outboxService) {
        this.aeOrderBuilderService = aeOrderBuilderService;
        this.aeOrderPersistenceService = aeOrderPersistenceService;
        this.butlerCoreGrpcClient = butlerCoreGrpcClient;
        this.outboxService = outboxService;
    }

    /**
     * Build AePickListRequest from the Kafka message, persist to ae_order table,
     * and publish to "pick-list.requests".
     *
     * Throws RuntimeException (aborting the Camunda task) if butler_server cannot be
     * reached or the matching orderline is not found — no AE order is created in that case.
     */
    @Transactional
    public void publishPickListRequest(PickInstructionRequestMessage msg) {
        AePickListRequest request = aeOrderBuilderService.build(msg);
        aeOrderPersistenceService.saveAeOrder(request);
        outboxService.save(pickListRequestsTopic, msg.getId(), request, "pick_list_request");
        log.info("Persisted AePickListRequest and queued to pick-list.requests for pickId: {}", msg.getId());
    }

    /**
     * Publish pick-instruction.response to notify butler_server of validation outcome.
     */
    @Transactional
    public void publishPickInstructionResponse(String pickId, String status,
            String orderId, String orderlineId, String message, String errorCode, String errorsJson) {
        boolean success = "SUCCESS".equalsIgnoreCase(status);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        if (success && orderlineId != null) {
            response.put("orderline_id", orderlineId);
        }
        if (!success) {
            if (errorCode != null) response.put("errorCode", errorCode);
            if (errorsJson != null) {
                try {
                    response.put("errors", objectMapper.readValue(errorsJson, List.class));
                } catch (Exception e) {
                    log.warn("Failed to parse errorsJson for pickId: {}", pickId, e);
                }
            }
        }
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Queued pick-instruction.response for pickId: {} | status: {}", pickId, status);
    }

    /**
     * Atomically delete the AE order from Postgres and publish a failure response to
     * "pick-instruction.response" via the transactional outbox — both in a single transaction.
     *
     * Called on the BPMN validation-failure path. Mirrors the pattern in publishPickListRequest
     * where AE order persistence and outbox write are atomic.
     */
    @Transactional
    public void terminateWithFailureResponse(String pickId, String status, String orderId,
            String message, String errorCode, String errorsJson) {
        aeOrderPersistenceService.markAsFailed(pickId);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        if (errorCode != null) response.put("errorCode", errorCode);
        if (errorsJson != null) {
            try {
                response.put("errors", objectMapper.readValue(errorsJson, List.class));
            } catch (Exception e) {
                log.warn("Failed to parse errorsJson for pickId: {}", pickId, e);
            }
        }
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Marked AE order as FAILED and queued failure response | pickId: {}", pickId);
    }

    /**
     * Update pick instruction via Butler Core gRPC, with retry and duplicate detection.
     */
    public UpdatePickInstructionResult updatePickInstruction(UpdatePickInstructionDto dto) {
        log.info("Updating pick instruction via Butler Core gRPC - orderId: {}, transactionId: {}",
                dto.getOrderId(), dto.getTransactionId());

        String txId = dto.getTransactionId();
        int attempt = 0;
        UpdatePickInstructionResult result = UpdatePickInstructionResult.permanentError("UNKNOWN", "No result");

        while (true) {
            attempt++;
            result = butlerCoreGrpcClient.updatePickInstruction(dto);

            log.info("Pick instruction update result - orderId: {}, status: {}, success: {}, retriable: {}, errorCode: {}",
                    dto.getOrderId(), result.getStatus(), result.isSuccess(),
                    result.isRetriable(), result.getErrorCode());

            if (result.isSuccess()) {
                aeOrderPersistenceService.saveTransactionStatus(txId, dto.getOrderId(), "SUCCESS");
                return result;
            }

            // Duplicate / already-exists → treat as success
            String errorCode = result.getErrorCode();
            String message = result.getMessage() != null ? result.getMessage().toLowerCase() : "";
            if ("ALREADY_EXISTS".equalsIgnoreCase(errorCode)
                    || message.contains("duplicate")
                    || message.contains("already exists")) {
                log.warn("Butler Core indicates duplicate transaction (treated as success) - txId: {}", txId);
                aeOrderPersistenceService.saveTransactionStatus(txId, dto.getOrderId(), "SUCCESS");
                return UpdatePickInstructionResult.ok("SUCCESS", "Duplicate treated as success: " + result.getMessage());
            }

            // Retriable and attempts remain → back off and retry
            if (result.isRetriable() && attempt <= UPDATE_MAX_RETRIES) {
                long backoff = Math.min(RETRY_BASE_DELAY_MS * (1L << (attempt - 1)), RETRY_MAX_DELAY_MS);
                log.warn("Retriable error from Butler Core — attempt {}/{}; backing off {} ms; txId: {}. Error: {}",
                        attempt, UPDATE_MAX_RETRIES, backoff, txId, result.getMessage());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while backing off before retry", ie);
                    break;
                }
                continue;
            }

            // Permanent failure or retries exhausted
            aeOrderPersistenceService.saveTransactionStatus(txId, dto.getOrderId(), "FAILED");
            break;
        }

        return result;
    }

    /**
     * Check whether a pick instruction is already complete via Butler Core.
     */
    public boolean isPickInstructionComplete(String pickId) {
        log.info("Checking pick instruction completion status - pickId: {}", pickId);
        try {
            GetPickInstructionStatusResponse statusResponse = butlerCoreGrpcClient.getPickInstructionStatus(pickId);
            log.info("Pick instruction status - pickId: {}, status: {}, isComplete: {}, totalQty: {}, processedQty: {}, remainingQty: {}",
                    pickId,
                    statusResponse.getStatus(),
                    statusResponse.getIsComplete(),
                    statusResponse.getTotalQty(),
                    statusResponse.getProcessedQty(),
                    statusResponse.getRemainingQty());
            return statusResponse.getIsComplete();
        } catch (Exception e) {
            log.error("Failed to check pick instruction status - pickId: {}", pickId, e);
            throw new RuntimeException("Failed to get pick instruction status: " + e.getMessage(), e);
        }
    }

    /**
     * Mark the pick instruction as complete.
     */
    public void markPickInstructionComplete(String pickId) {
        log.info("Finalizing Workflow: Pick Instruction {} is COMPLETE.", pickId);
    }

    /**
     * Mark the pick instruction as failed.
     */
    public void markPickInstructionFailed(String pickId, String transactionId, String failureReason) {
        log.error("Pick Instruction FAILED - pickId: {}, transactionId: {}, reason: {}",
                pickId, transactionId, failureReason);
    }

    /**
     * Send transaction details to Kafka for downstream consumers.
     */
    @Transactional
    public void sendTransactionKafkaEvent(TransactionUpdate transactionUpdate) {
        log.info("Sending transaction event to Kafka for pickId: {}, transactionId: {}",
                transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        outboxService.save(transactionEventsTopic, transactionUpdate.getPickId(), transactionUpdate, "transaction_event");
        log.info("Queued Transaction Event for pickId: {}", transactionUpdate.getPickId());
    }

    /**
     * Process the transaction update — duplicate detection, in-progress tracking, completion check.
     * Returns true if the pick is now complete.
     * Transactional to ensure atomic duplicate detection (find + save).
     */
    @Transactional
    public boolean processTransactionUpdate(String pickId, TransactionUpdate transactionUpdate) {
        log.info("Processing transaction update for pickId: {}, transactionId: {}, type: {}",
                pickId, transactionUpdate.getTransactionId(), transactionUpdate.getTransactionType());

        String txId = transactionUpdate.getTransactionId();

        if (txId != null && !txId.isEmpty()) {
            try {
                Optional<TransactionStatus> existing = aeOrderPersistenceService.findTransactionStatus(txId);
                if (existing.isPresent()) {
                    String status = existing.get().getStatus();
                    if ("SUCCESS".equals(status)) {
                        log.info("Duplicate transaction detected (persisted) - ignoring txId: {} for pickId: {}", txId, pickId);
                        return "COMPLETED".equalsIgnoreCase(transactionUpdate.getStatus());
                    }
                    if ("IN_PROGRESS".equals(status)) {
                        log.info("Transaction already in progress (persisted) - txId: {} for pickId: {}", txId, pickId);
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to read persisted transaction status for txId: {} — falling back to processing", txId, e);
            }
            aeOrderPersistenceService.saveTransactionStatus(txId, pickId, "IN_PROGRESS");
        }

        int attempts = 0;
        int maxAttempts = 3;
        while (true) {
            attempts++;
            try {
                boolean isComplete = "COMPLETED".equalsIgnoreCase(transactionUpdate.getStatus());
                aeOrderPersistenceService.saveTransactionStatus(txId, pickId, "SUCCESS");
                log.info("Transaction processed for PI: {}, Status: {}, Complete: {}",
                        pickId, transactionUpdate.getStatus(), isComplete);
                return isComplete;
            } catch (Exception e) {
                log.warn("Error processing transaction update for pickId: {}, txId: {}, attempt: {}",
                        pickId, txId, attempts, e);
                if (attempts >= maxAttempts) {
                    log.error("Exceeded max attempts processing transaction update for pickId: {}, txId: {}", pickId, txId);
                    aeOrderPersistenceService.saveTransactionStatus(txId, pickId, "FAILED");
                    try {
                        markFailureInButlerCore(pickId, "PROCESSING_FAILED", e.getMessage());
                    } catch (Exception ignore) {
                        log.warn("Failed to notify Butler Core of processing failure for pickId: {}", pickId, ignore);
                    }
                    return false;
                }
                try {
                    Thread.sleep(Math.min(500L * attempts, RETRY_MAX_DELAY_MS));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted during processing backoff", ie);
                    aeOrderPersistenceService.saveTransactionStatus(txId, pickId, "FAILED");
                    return false;
                }
            }
        }
    }

    /**
     * Notify Butler Core of a processing failure for this pick instruction.
     */
    public void markFailureInButlerCore(String pickId, String errorCode, String errorMessage) {
        log.error("Marking failure in Butler Core for pickId: {}, errorCode: {}, errorMessage: {}",
                pickId, errorCode, errorMessage);
        try {
            UpdatePickInstructionDto failureDto = UpdatePickInstructionDto.builder()
                    .orderId(pickId)
                    .build();
            UpdatePickInstructionResult result = butlerCoreGrpcClient.updatePickInstruction(failureDto);
            log.info("Butler Core failure notification result for pickId: {} — status: {}, success: {}",
                    pickId, result.getStatus(), result.isSuccess());
        } catch (Exception e) {
            log.warn("Failed to notify Butler Core of failure for pickId: {}. Non-critical.", pickId, e);
        }
    }

    /**
     * Run workflow completion cleanup: status validation against Butler Core and Kafka audit event.
     */
    @Transactional
    public void onWorkflowComplete(String pickId, String internalStatus, String failureReason) {
        log.info("Workflow completing for pickId: {} — running cleanup and validation. internalStatus: {}",
                pickId, internalStatus);

        String externalStatus = null;
        boolean externalIsComplete = false;
        try {
            GetPickInstructionStatusResponse externalResponse = butlerCoreGrpcClient.getPickInstructionStatus(pickId);
            externalStatus = externalResponse.getStatus();
            externalIsComplete = externalResponse.getIsComplete();
            log.info("External status from Butler Core — pickId: {}, status: {}, isComplete: {}, " +
                            "totalQty: {}, processedQty: {}, remainingQty: {}",
                    pickId, externalStatus, externalIsComplete,
                    externalResponse.getTotalQty(),
                    externalResponse.getProcessedQty(),
                    externalResponse.getRemainingQty());
        } catch (Exception e) {
            log.warn("Failed to fetch external status from Butler Core for pickId: {}. " +
                    "Proceeding with cleanup using internal status only.", pickId, e);
        }

        if (externalStatus != null) {
            boolean statusMismatch = false;
            if ("COMPLETED".equals(internalStatus) && !externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is COMPLETED internally but NOT complete in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            } else if ("FAILED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is FAILED internally but COMPLETE in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            } else if ("CANCELLED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is CANCELLED internally but COMPLETE in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            }
            if (!statusMismatch) {
                log.info("Status validation PASSED — pickId: {} internal [{}] consistent with external [{}]",
                        pickId, internalStatus, externalStatus);
            }
        }

        try {
            Map<String, Object> auditEvent = new LinkedHashMap<>();
            auditEvent.put("pickId", pickId);
            auditEvent.put("internalStatus", internalStatus);
            auditEvent.put("externalStatus", externalStatus);
            auditEvent.put("externalIsComplete", externalIsComplete);
            auditEvent.put("failureReason", failureReason);
            auditEvent.put("timestamp", Instant.now().toString());
            outboxService.save(workflowCompleteEventsTopic, pickId, auditEvent, "workflow_complete");
            log.info("Queued workflow-complete audit event for pickId: {}", pickId);
        } catch (Exception e) {
            log.warn("Failed to publish workflow-complete audit event for pickId: {}. Non-critical.", pickId, e);
        }

        log.info("Workflow cleanup complete for pickId: {} — final status: {}", pickId, internalStatus);
    }
}
