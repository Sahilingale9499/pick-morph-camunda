package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.SrmsPickListResponse;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
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
 *   errorCode, message → stored as process variables for observability
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

        String pickId = srmsResponse.getExternalServiceRequestId();
        if (pickId == null) {
            log.error("Received SRMS response with null externalServiceRequestId — dropping message");
            return;
        }

        boolean success = "SUCCESS".equalsIgnoreCase(srmsResponse.getStatus());
        log.info("SRMS pick-list.response | pickId: {} | status: {} | errorCode: {}",
                pickId, srmsResponse.getStatus(), srmsResponse.getErrorCode());

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

            log.info("PickListResponseMessage correlated successfully for orderId: {} | success: {}",
                    pickId, success);

        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance found waiting for PickListResponseMessage for orderId: {}. " +
                    "SRMS response dropped.", pickId);
        } catch (Exception e) {
            log.error("Failed to correlate PickListResponseMessage for orderId: {} | error: {}",
                    pickId, e.getMessage(), e);
        }
    }
}
