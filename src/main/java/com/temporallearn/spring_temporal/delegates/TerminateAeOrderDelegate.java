package com.temporallearn.spring_temporal.delegates;

import com.temporallearn.spring_temporal.repository.AeOrderRepository;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Deletes the AE order from PostgreSQL when validation fails.
 *
 * Called from the BPMN validation-failure path after PublishPickInstructionResponseDelegate
 * has already notified butler_server of the failure via pick-instruction.response.
 */
@Component
@Slf4j
public class TerminateAeOrderDelegate implements JavaDelegate {

    private final AeOrderRepository aeOrderRepository;

    public TerminateAeOrderDelegate(AeOrderRepository aeOrderRepository) {
        this.aeOrderRepository = aeOrderRepository;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId = (String) execution.getVariable("pickId");
        aeOrderRepository.findByExternalServiceRequestId(pickId)
                .ifPresent(order -> aeOrderRepository.deleteById(order.getId()));
        log.info("AE order terminated in PostgreSQL | pickId: {}", pickId);
    }
}
