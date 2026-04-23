package greymatter.butler.aeorder.delegates;

import greymatter.butler.aeorder.service.PickInstructionService;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Atomically publishes a failure response to "pick-instruction.response" and
 * deletes the AE order from PostgreSQL in a single transaction.
 *
 * Called exclusively on the BPMN validation-failure path (validationSuccess = false).
 */
@Component
@Slf4j
public class HandleValidationFailureDelegate {

    private final PickInstructionService pickInstructionService;

    public HandleValidationFailureDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @JobWorker(type = "handle-validation-failure")
    public void handleFailure(
            @Variable String pickId,
            @Variable String validationStatus,
            @Variable String orderId,
            @Variable String orderlineId,
            @Variable String validationMessage,
            @Variable String validationErrorCode,
            @Variable String validationErrors) throws Exception {
        pickInstructionService.terminateWithFailureResponse(
                pickId, validationStatus, orderId, orderlineId,
                validationMessage, validationErrorCode, validationErrors);
        log.info("Failure response queued and AE order deleted atomically | pickId: {}", pickId);
    }
}
