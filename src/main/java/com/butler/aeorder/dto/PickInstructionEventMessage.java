package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Envelope DTO for messages consumed from the "pick-instruction.requests" Kafka topic.
 * The actual pick instruction data is in {@link #payload}; the envelope carries routing
 * and tracing metadata. {@code entity_id} is the Kafka message key used by the producer
 * to ensure all events for the same pick instruction land on the same partition (ordering).
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PickInstructionEventMessage {

    private String sourceService;
    private String timestamp;
    private String messageId;
    private String entityId;       // pick_instruction_id — Kafka partition key for ordering
    private String name;
    private PickInstructionRequestMessage payload;
    private Context context;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Context {
        private String executionId;
    }
}
