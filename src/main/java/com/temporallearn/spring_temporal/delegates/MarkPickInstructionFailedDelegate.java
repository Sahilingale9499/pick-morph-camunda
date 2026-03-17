package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Marks the pick instruction as failed/cancelled, and sets "finalStatus" and
 * "finalFailureReason" process variables so that OnWorkflowCompleteDelegate
 * can read them.
 *
 * Uses the "command" process variable to distinguish CANCEL (→ "CANCELLED")
 * from all other failure paths (→ "FAILED").
 */
@Component
@Slf4j
public class MarkPickInstructionFailedDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public MarkPickInstructionFailedDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        String transactionId = (String) execution.getVariable("transactionId");
        String failureReason = (String) execution.getVariable("failureReason");
        String command = (String) execution.getVariable("command");

        log.info("MarkPickInstructionFailedDelegate executing for pickId: {}, command: {}, transactionId: {}",
                pickId, command, transactionId);

        pickInstructionService.markPickInstructionFailed(pickId, transactionId, failureReason);

        String finalStatus = "CANCEL".equalsIgnoreCase(command) ? "CANCELLED" : "FAILED";
        execution.setVariable("finalStatus", finalStatus);
        execution.setVariable("finalFailureReason", failureReason);
    }
}
