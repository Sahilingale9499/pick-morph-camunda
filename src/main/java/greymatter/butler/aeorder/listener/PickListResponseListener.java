package greymatter.butler.aeorder.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.ae.PickListResponseEvent;
import io.camunda.zeebe.client.ZeebeClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Step 3 — Kafka listener for AE pick-list response messages.
 *
 * Consumes messages from gor.pick-list.response published by AE after
 * processing a pick-list request sent in Step 2.
 *
 * Publishes a Zeebe message to advance the waiting process instance past
 * "Wait for Validation". orderId/orderlineId are read directly from
 * process-start variables inside the downstream job worker.
 */
@Service
@Slf4j
public class PickListResponseListener {

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topic.pick-list-response}",
            groupId = "pick-list-response-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickListResponseEvent.Payload response;
        try {
            PickListResponseEvent envelope = objectMapper.readValue(payload, PickListResponseEvent.class);
            if (envelope.getPayload() == null) {
                log.error("PickListResponseEvent has no payload — dropping message");
                return;
            }
            response = envelope.getPayload();
        } catch (Exception e) {
            log.error("Failed to deserialize PickListResponseEvent from Kafka message: {}", e.getMessage());
            return;
        }

        boolean success = response.isSuccess();

        log.info("Received pick-list response from AE | externalServiceRequestId: {} | status: {} | success: {}",
                response.getExternalServiceRequestId(),
                response.getStatus(),
                success);

        if (!success) {
            log.warn("AE returned FAILURE for pickInstructionId: {} | errorCode: {} | message: {}",
                    response.getExternalServiceRequestId(),
                    response.getErrorCode(),
                    response.getMessage());
        }

        try {
            String errorsJson = null;
            if (response.getErrors() != null && !response.getErrors().isEmpty()) {
                errorsJson = objectMapper.writeValueAsString(response.getErrors());
            }

            Map<String, Object> msgVars = new HashMap<>();
            msgVars.put("validationSuccess", success);
            msgVars.put("validationStatus", success ? "SUCCESS" : "FAILURE");
            msgVars.put("validationMessage", response.getMessage());
            msgVars.put("validationErrorCode", response.getErrorCode());
            msgVars.put("validationErrors", errorsJson);

            zeebeClient.newPublishMessageCommand()
                    .messageName("PickListResponseMessage")
                    .correlationKey(response.getExternalServiceRequestId())
                    .variables(msgVars)
                    .timeToLive(Duration.ofMinutes(5))
                    .send()
                    .join();

            log.info("PickListResponseMessage published to Zeebe for pickInstructionId: {} | success: {}",
                    response.getExternalServiceRequestId(), success);

        } catch (Exception e) {
            log.error("Failed to publish PickListResponseMessage for pickInstructionId: {} | error: {}",
                    response.getExternalServiceRequestId(), e.getMessage(), e);
        }
    }
}
