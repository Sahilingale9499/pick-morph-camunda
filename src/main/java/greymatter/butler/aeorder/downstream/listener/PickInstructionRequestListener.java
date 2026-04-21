package greymatter.butler.aeorder.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstructionRequestMessage;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.service.PickInstructionProcessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka listener for pick instruction events published by butler_server.
 *
 * Consumes messages from "pick-instruction.requests" and starts a Camunda process.
 * The process is responsible for constructing the AE order payload, persisting it
 * to PostgreSQL, and publishing it to "pick-list.requests".
 *
 * Field semantics:
 *   id            → pick_instruction_id = AE externalServiceRequestId (workflow/order PK)
 *   order_id      → normal/main order ID
 *   orderline_id  → normal/main orderline ID
 *   tpid          → product type ID (integer) — gRPC use only, NOT the product SKU
 */
@Slf4j
@Service
public class PickInstructionRequestListener {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PickInstructionProcessService processService;

    @KafkaListener(
            topics = "${kafka.topics.pick-instructions-request}",
            groupId = "pick-instruction-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickInstructionRequestMessage event;
        try {
            event = objectMapper.readValue(payload, PickInstructionRequestMessage.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickInstructionRequestMessage: {}", e.getMessage());
            return;
        }

        PickInstruction msg = event.getPayload();

        String error = validationError(msg);
        if (error != null) {
            log.error("Received pick-instruction.requests with {} for pickId: {} — dropping", error, msg.getId());
            return;
        }

        log.info("Received pick-instruction | pickId: {} | orderId: {} | tpid: {} | ppsId: {}",
                msg.getId(), msg.getOrderId(), msg.getTpid(), msg.getPpsId());

        processService.startProcess(msg);
    }

    /**
     * Returns the first validation error for the message, or null if all fields are valid.
     * Extracted to enable unit testing of validation logic without Spring/Kafka context.
     */
    static String validationError(PickInstruction msg) {
        if (msg.getId() == null || msg.getId().isBlank())                   return "null/blank id";
        if (msg.getOrderId() == null || msg.getOrderId().isBlank())         return "null/blank orderId";
        if (msg.getOrderlineId() == null || msg.getOrderlineId().isBlank()) return "null/blank orderlineId";
        if (msg.getQty() <= 0)                                               return "invalid qty";
        if (msg.getSlotId() == null || msg.getSlotId().isBlank())           return "null/blank slotId";
        return null;
    }
}
