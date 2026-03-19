package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstructionRequestMessage;
import com.temporallearn.spring_temporal.service.PickInstructionProcessService;
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
            topics = "pick-instruction.requests",
            groupId = "pick-instruction-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickInstructionRequestMessage msg;
        try {
            msg = objectMapper.readValue(payload, PickInstructionRequestMessage.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickInstructionRequestMessage: {}", e.getMessage());
            return;
        }

        if (msg.getId() == null || msg.getId().isBlank()) {
            log.error("Received pick-instruction.requests message with null/blank id — dropping");
            return;
        }

        log.info("Received pick-instruction | pickId: {} | orderId: {} | tpid: {} | ppsId: {}",
                msg.getId(), msg.getOrderId(), msg.getTpid(), msg.getPpsId());

        processService.startProcess(msg);
    }
}
