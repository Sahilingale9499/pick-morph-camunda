package greymatter.butler.aeorder.delegates;

import greymatter.butler.aeorder.service.PickInstructionService;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Publishes the validation outcome to "pick-instruction.response" so that
 * butler_server is notified whether the AE order was accepted or rejected.
 * Uses the transactional outbox via PickInstructionService.
 *
 * orderId and orderlineId are read directly from process-start variables (no listener pre-fetch
 * needed in Camunda 8 — all process variables are visible to job workers).
 */
@Component
@Slf4j
public class PublishPickInstructionResponseDelegate {

    private final PickInstructionService pickInstructionService;

    public PublishPickInstructionResponseDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @JobWorker(type = "publish-pick-instruction-response")
    public void publishResponse(
            @Variable String pickId,
            @Variable String validationStatus,
            @Variable String orderId,
            @Variable String orderlineId,
            @Variable String validationMessage,
            @Variable String validationErrorCode,
            @Variable String validationErrors) throws Exception {
        pickInstructionService.publishPickInstructionResponse(
                pickId, validationStatus, orderId, orderlineId,
                validationMessage, validationErrorCode, validationErrors);
        log.info("Queued pick-instruction.response via outbox | pickId: {} | status: {}", pickId, validationStatus);
    }
}
