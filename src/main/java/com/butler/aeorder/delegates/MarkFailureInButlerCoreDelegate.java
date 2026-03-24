package com.butler.aeorder.delegates;

import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Notifies Butler Core of a non-retriable processing failure.
 * Reads "pickId", "updateErrorCode", and "updateErrorMessage" from process variables.
 */
@Component
@Slf4j
public class MarkFailureInButlerCoreDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public MarkFailureInButlerCoreDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        String errorCode = (String) execution.getVariable("updateErrorCode");
        String errorMessage = (String) execution.getVariable("updateErrorMessage");

        log.info("MarkFailureInButlerCoreDelegate executing for pickId: {}, errorCode: {}",
                pickId, errorCode);

        pickInstructionService.markFailureInButlerCore(pickId, errorCode, errorMessage);
    }
}
