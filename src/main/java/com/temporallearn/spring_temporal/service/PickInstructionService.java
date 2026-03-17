package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.Order;
import com.temporallearn.spring_temporal.dto.OrderLine;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.UpdatePickInstructionDto;
import com.temporallearn.spring_temporal.dto.UpdatePickInstructionResult;
import com.temporallearn.spring_temporal.grpc.ButlerCoreGrpcClient;
import com.temporallearn.spring_temporal.model.TransactionStatus;
import com.temporallearn.spring_temporal.repository.TransactionStatusRepository;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Business logic service extracted from PickActivitiesImpl.
 * Used by Camunda JavaDelegate classes to perform all domain operations.
 */
@Service
@Slf4j
public class PickInstructionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ButlerCoreGrpcClient butlerCoreGrpcClient;
    private final TransactionStatusRepository transactionStatusRepository;
    private final ObjectMapper objectMapper;

    private static final int UPDATE_MAX_RETRIES = 3;
    private static final long RETRY_BASE_DELAY_MS = 1000L;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  ButlerCoreGrpcClient butlerCoreGrpcClient,
                                  TransactionStatusRepository transactionStatusRepository,
                                  ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.butlerCoreGrpcClient = butlerCoreGrpcClient;
        this.transactionStatusRepository = transactionStatusRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Transform PickInstruction to Order and publish to Kafka.
     */
    public void publishOrderToKafka(PickInstruction pi) {
        Order order = new Order();
        order.setOrderId(pi.getPickId());
        OrderLine line = new OrderLine();
        line.setPickInstructionId(pi.getPickId());
        line.setItem(pi.getItem());
        line.setQty(pi.getQty());
        line.setUom(pi.getUom());
        line.setScannableBarcodes(pi.getScannableBarcodes());
        line.setPickLocation(pi.getPickLocation());
        line.setDropLocation(pi.getDropLocation());
        order.setOrderLines(List.of(line));

        kafkaTemplate.send("orders-topic", order.getOrderId(), order);
        log.info("Published Order to Kafka: {}", order.getOrderId());
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
                persistTransactionStatus(txId, dto.getOrderId(), "SUCCESS");
                return result;
            }

            // Duplicate / already-exists → treat as success
            String errorCode = result.getErrorCode();
            String message = result.getMessage() != null ? result.getMessage().toLowerCase() : "";
            if ("ALREADY_EXISTS".equalsIgnoreCase(errorCode)
                    || message.contains("duplicate")
                    || message.contains("already exists")) {
                log.warn("Butler Core indicates duplicate transaction (treated as success) - txId: {}", txId);
                persistTransactionStatus(txId, dto.getOrderId(), "SUCCESS");
                return UpdatePickInstructionResult.ok("SUCCESS", "Duplicate treated as success: " + result.getMessage());
            }

            // Retriable and attempts remain → back off and retry
            if (result.isRetriable() && attempt <= UPDATE_MAX_RETRIES) {
                long backoff = RETRY_BASE_DELAY_MS * (1L << (attempt - 1));
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
            persistTransactionStatus(txId, dto.getOrderId(), "FAILED");
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
        // repository.updateStatus(pickId, "COMPLETED");
    }

    /**
     * Mark the pick instruction as failed.
     */
    public void markPickInstructionFailed(String pickId, String transactionId, String failureReason) {
        log.error("Pick Instruction FAILED - pickId: {}, transactionId: {}, reason: {}",
                pickId, transactionId, failureReason);
        // repository.updateStatus(pickId, "FAILED");
        // repository.setFailureReason(pickId, failureReason);
    }

    /**
     * Send transaction details to Kafka for downstream consumers.
     */
    public void sendTransactionKafkaEvent(TransactionUpdate transactionUpdate) {
        log.info("Sending transaction event to Kafka for pickId: {}, transactionId: {}",
                transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        kafkaTemplate.send("transaction-events-topic",
                transactionUpdate.getPickId(),
                transactionUpdate);
        log.info("Published Transaction Event to Kafka: {}", transactionUpdate.getTransactionId());
    }

    /**
     * Process the transaction update — duplicate detection, in-progress tracking, completion check.
     * Returns true if the pick is now complete.
     */
    public boolean processTransactionUpdate(String pickId, TransactionUpdate transactionUpdate) {
        log.info("Processing transaction update for pickId: {}, transactionId: {}, type: {}",
                pickId, transactionUpdate.getTransactionId(), transactionUpdate.getTransactionType());

        String txId = transactionUpdate.getTransactionId();

        if (txId != null && !txId.isEmpty()) {
            try {
                Optional<TransactionStatus> existing = transactionStatusRepository.findById(txId);
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
            persistTransactionStatus(txId, pickId, "IN_PROGRESS");
        }

        int attempts = 0;
        int maxAttempts = 3;
        while (true) {
            attempts++;
            try {
                boolean isComplete = "COMPLETED".equalsIgnoreCase(transactionUpdate.getStatus());
                persistTransactionStatus(txId, pickId, "SUCCESS");
                log.info("Transaction processed for PI: {}, Status: {}, Complete: {}",
                        pickId, transactionUpdate.getStatus(), isComplete);
                return isComplete;
            } catch (Exception e) {
                log.warn("Error processing transaction update for pickId: {}, txId: {}, attempt: {}",
                        pickId, txId, attempts, e);
                if (attempts >= maxAttempts) {
                    log.error("Exceeded max attempts processing transaction update for pickId: {}, txId: {}", pickId, txId);
                    persistTransactionStatus(txId, pickId, "FAILED");
                    try {
                        markFailureInButlerCore(pickId, "PROCESSING_FAILED", e.getMessage());
                    } catch (Exception ignore) {
                        log.warn("Failed to notify Butler Core of processing failure for pickId: {}", pickId, ignore);
                    }
                    return false;
                }
                try {
                    Thread.sleep(500L * attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted during processing backoff", ie);
                    persistTransactionStatus(txId, pickId, "FAILED");
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
            kafkaTemplate.send("workflow-complete-events-topic", pickId, auditEvent);
            log.info("Published workflow-complete audit event to Kafka for pickId: {}", pickId);
        } catch (Exception e) {
            log.warn("Failed to publish workflow-complete audit event for pickId: {}. Non-critical.", pickId, e);
        }

        log.info("Workflow cleanup complete for pickId: {} — final status: {}", pickId, internalStatus);
    }

    // ─── Internal helper ────────────────────────────────────────────────────

    private void persistTransactionStatus(String txId, String pickId, String status) {
        if (txId == null || txId.isEmpty()) {
            return;
        }
        try {
            TransactionStatus ts = new TransactionStatus(txId, pickId, status, Instant.now());
            transactionStatusRepository.save(ts);
        } catch (Exception e) {
            log.warn("Failed to persist transaction status {} for txId: {}", status, txId, e);
        }
    }
}
