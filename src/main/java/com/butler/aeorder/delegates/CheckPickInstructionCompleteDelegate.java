package com.butler.aeorder.delegates;

import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Calls Butler Core to check whether the pick instruction is already complete
 * and writes the result to the "isAlreadyComplete" process variable.
 */
@Component
@Slf4j
public class CheckPickInstructionCompleteDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public CheckPickInstructionCompleteDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        log.info("CheckPickInstructionCompleteDelegate executing for pickId: {}", pickId);
        boolean isComplete = pickInstructionService.isPickInstructionComplete(pickId);
        execution.setVariable("isAlreadyComplete", isComplete);
        log.info("isAlreadyComplete={} for pickId: {}", isComplete, pickId);
    }
}
