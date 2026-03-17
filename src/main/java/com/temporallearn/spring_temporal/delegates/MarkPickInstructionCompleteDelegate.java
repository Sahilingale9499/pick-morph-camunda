package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Marks the pick instruction as complete and sets the "finalStatus" process variable
 * to "COMPLETED" so that OnWorkflowCompleteDelegate can read it.
 */
@Component
@Slf4j
public class MarkPickInstructionCompleteDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public MarkPickInstructionCompleteDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        log.info("MarkPickInstructionCompleteDelegate executing for pickId: {}", pickId);
        pickInstructionService.markPickInstructionComplete(pickId);
        execution.setVariable("finalStatus", "COMPLETED");
        execution.setVariable("finalFailureReason", null);
    }
}
