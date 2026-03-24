package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.SrmsPickListResponse;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka listener for SRMS pick-list validation responses.
 *
 * Consumes messages from "pick-list.response" published by SRMS after processing
 * an AE order from pick-list.requests. Maps the SRMS response to a Camunda
 * "PickListResponseMessage" correlation so the process can proceed past the
 * validation wait point.
 *
 * After correlation, the BPMN executes PublishPickInstructionResponseDelegate
 * which publishes the outcome to "pick-instruction.response" for butler_server.
 *
 * Field mapping:
 *   externalServiceRequestId → pickId (process variable used for correlation)
 *   status "SUCCESS"/"FAILURE" → validationSuccess process variable
 *   serviceRequestId → validated against stored orderId from pick-instruction.requests
 *   errorCode, message → stored as process variables for observability
 *
 * Idempotency: pre-checks process existence before correlation; try-catch handles
 * the race where another thread correlates between check and correlate.
 */
@Slf4j
@Service
public class PickListResponseListener {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(
            topics = "pick-list.response",
            groupId = "pick-list-response-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        SrmsPickListResponse srmsResponse;
        try {
            srmsResponse = objectMapper.readValue(payload, SrmsPickListResponse.class);
        } catch (Exception e) {
            log.error("Failed to deserialize SrmsPickListResponse: {}", e.getMessage());
            return;
        }

        // --- Field validation ---

        String pickId = srmsResponse.getExternalServiceRequestId();
        if (pickId == null || pickId.isBlank()) {
            log.error("Received SRMS response with null/blank externalServiceRequestId — dropping message");
            return;
        }

        String status = srmsResponse.getStatus();
        if (status == null || status.isBlank()) {
            log.error("Received SRMS response with null/blank status for pickId: {} — dropping", pickId);
            return;
        }
        if (!"SUCCESS".equalsIgnoreCase(status) && !"FAILURE".equalsIgnoreCase(status)) {
            log.error("Received SRMS response with invalid status: '{}' for pickId: {} — must be SUCCESS or FAILURE — dropping",
                    status, pickId);
            return;
        }

        if (srmsResponse.getServiceRequestId() == null) {
            log.error("Received SRMS response with null serviceRequestId for pickId: {} — dropping", pickId);
            return;
        }

        // --- Pre-check: does a process instance exist for this pickId? ---

        ProcessInstance pi = runtimeService.createProcessInstanceQuery()
                .variableValueEquals("pickId", pickId)
                .singleResult();
        if (pi == null) {
            log.warn("pick-list.response for pickId: {} — no process instance found (out-of-order or stale) — dropping",
                    pickId);
            return;
        }

        // --- Cross-reference: validate serviceRequestId matches the stored orderId ---

        String storedOrderId = (String) runtimeService.getVariable(pi.getId(), "orderId");
        if (storedOrderId != null
                && !storedOrderId.equals(srmsResponse.getServiceRequestId())) {
            log.error("serviceRequestId mismatch for pickId: {} — expected orderId: {}, got: {} — dropping",
                    pickId, storedOrderId, srmsResponse.getServiceRequestId());
            return;
        }

        // --- Correlate message to the waiting process ---

        boolean success = "SUCCESS".equalsIgnoreCase(status);
        log.info("SRMS pick-list.response | pickId: {} | status: {} | errorCode: {}",
                pickId, status, srmsResponse.getErrorCode());

        String failureReason = success ? null
                : (srmsResponse.getErrorCode() != null
                        ? srmsResponse.getErrorCode() + ": " + srmsResponse.getMessage()
                        : "Validation failed");

        try {
            runtimeService.createMessageCorrelation("PickListResponseMessage")
                    .processInstanceVariableEquals("pickId", pickId)
                    .setVariable("validationSuccess", success)
                    .setVariable("validationResult", pickId)
                    .setVariable("validationErrorCode", srmsResponse.getErrorCode())
                    .setVariable("validationMessage", srmsResponse.getMessage())
                    .setVariable("failureReason", failureReason)
                    .correlate();

            log.info("PickListResponseMessage correlated successfully for pickId: {} | success: {}",
                    pickId, success);

        } catch (MismatchingMessageCorrelationException e) {
            // Race condition: another message correlated between our pre-check and correlate(),
            // or process already moved past waitForValidation. Camunda guarantees exactly-once
            // correlation per message subscription, so this is safe to skip.
            log.info("PickListResponseMessage already processed for pickId: {} — duplicate or race (safe to skip)",
                    pickId);
        } catch (Exception e) {
            log.error("Failed to correlate PickListResponseMessage for pickId: {} | error: {}",
                    pickId, e.getMessage(), e);
        }
    }
}
