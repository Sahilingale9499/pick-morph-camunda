package com.temporallearn.spring_temporal.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

/**
 * Published to "pick-instruction.response" after receiving a validation result
 * from SRMS on "pick-list.response". Consumed by butler_server to update the
 * pick instruction status.
 */
@Data
@Builder
public class PickInstructionResponseMessage {

    private String id;      // pick_instruction_id

    private String status;  // "success" or "failure"
}
