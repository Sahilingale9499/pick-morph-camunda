package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Terminal cleanup delegate — always invoked at the end of every process path.
 * Reads "pickId", "finalStatus", and "finalFailureReason" from process variables,
 * then delegates to PickInstructionService.onWorkflowComplete() for status
 * validation against Butler Core and audit event publication to Kafka.
 */
@Component
@Slf4j
public class OnWorkflowCompleteDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public OnWorkflowCompleteDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        String finalStatus = (String) execution.getVariable("finalStatus");
        String failureReason = (String) execution.getVariable("finalFailureReason");

        log.info("OnWorkflowCompleteDelegate executing for pickId: {}, finalStatus: {}",
                pickId, finalStatus);

        pickInstructionService.onWorkflowComplete(pickId, finalStatus, failureReason);
    }
}
