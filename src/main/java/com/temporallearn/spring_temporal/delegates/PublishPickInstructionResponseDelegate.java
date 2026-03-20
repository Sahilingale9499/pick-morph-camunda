package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Publishes the validation outcome to "pick-instruction.response" so that
 * butler_server is notified whether the AE order was accepted or rejected.
 * Uses the transactional outbox via PickInstructionService.
 *
 * Reads process variables:
 *   pickId           - pick instruction ID
 *   validationSuccess - Boolean set by PickListResponseListener before correlating
 */
@Component
@Slf4j
public class PublishPickInstructionResponseDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public PublishPickInstructionResponseDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        Boolean validationSuccess = (Boolean) execution.getVariable("validationSuccess");
        boolean success = Boolean.TRUE.equals(validationSuccess);

        pickInstructionService.publishPickInstructionResponse(pickId, success);
        log.info("Queued pick-instruction.response via outbox | pickId: {} | success: {}", pickId, success);
    }
}
