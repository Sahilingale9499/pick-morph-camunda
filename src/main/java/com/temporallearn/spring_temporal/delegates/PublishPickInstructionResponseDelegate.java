package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.dto.PickInstructionResponseMessage;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes the validation outcome to "pick-instruction.response" so that
 * butler_server is notified whether the AE order was accepted or rejected.
 *
 * Reads process variables:
 *   pickId           - pick instruction ID
 *   validationSuccess - Boolean set by PickListResponseListener before correlating
 */
@Component
@Slf4j
public class PublishPickInstructionResponseDelegate implements JavaDelegate {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PublishPickInstructionResponseDelegate(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        Boolean validationSuccess = (Boolean) execution.getVariable("validationSuccess");
        boolean success = Boolean.TRUE.equals(validationSuccess);

        PickInstructionResponseMessage msg = PickInstructionResponseMessage.builder()
                .id(pickId)
                .status(success ? "success" : "failure")
                .build();

        kafkaTemplate.send("pick-instruction.response", pickId, msg);
        log.info("Published pick-instruction.response | pickId: {} | status: {}", pickId, msg.getStatus());
    }
}
