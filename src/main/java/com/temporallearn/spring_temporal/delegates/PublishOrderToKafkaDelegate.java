package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Deserializes PickInstruction from the "instructionJson" process variable
 * and publishes the corresponding Order to Kafka.
 */
@Component
@Slf4j
public class PublishOrderToKafkaDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public PublishOrderToKafkaDelegate(PickInstructionService pickInstructionService,
                                       ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String instructionJson = (String) execution.getVariable("instructionJson");
        PickInstruction instruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        log.info("PublishOrderToKafkaDelegate executing for pickId: {}", instruction.getPickId());
        pickInstructionService.publishOrderToKafka(instruction);
    }
}
