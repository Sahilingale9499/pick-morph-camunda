package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Deserializes TransactionUpdate from the "transactionUpdateJson" process variable
 * and publishes the event to the transaction-events Kafka topic.
 */
@Component
@Slf4j
public class SendTransactionKafkaEventDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public SendTransactionKafkaEventDelegate(PickInstructionService pickInstructionService,
                                              ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String transactionUpdateJson = (String) execution.getVariable("transactionUpdateJson");
        TransactionUpdate transactionUpdate = objectMapper.readValue(transactionUpdateJson, TransactionUpdate.class);
        log.info("SendTransactionKafkaEventDelegate executing for pickId: {}, transactionId: {}",
                transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        pickInstructionService.sendTransactionKafkaEvent(transactionUpdate);
    }
}
