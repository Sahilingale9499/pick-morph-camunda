package greymatter.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.dto.ae.PickListEvent;
import greymatter.butler.aeorder.service.PickInstructionService;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Unified delegate that processes every pick-list event regardless of event_type.
 *
 * Delegates all business logic to {@link PickInstructionService#processPickListEvent}, which:
 *   1. Updates ae_order.
 *   2. For every transaction whose container status is new (dedup check):
 *        loaded / complete  → ItemPickedEvent (danglingArea = "bot")
 *        unloaded           → ItemPickedEvent (danglingArea = null)
 *        created            → OrderUpdateEvent (transaction data as actuals)
 *   3. Enqueues an SR-level OrderUpdateEvent.
 *
 * Sole owner of the command routing decision:
 *   command = "COMPLETE" when derived order status is "released" (ends the workflow)
 *   command = "UPDATE"   otherwise (loops back to wait for the next event)
 */
@Component
@Slf4j
public class ProcessPickListEventDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public ProcessPickListEventDelegate(PickInstructionService pickInstructionService,
                                        ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @JobWorker(type = "process-pick-list-event")
    public Map<String, Object> processEvent(
            @Variable String pickInstructionId,
            @Variable String instructionJson,
            @Variable String pickListEventJson) throws Exception {
        PickInstruction pickInstruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        PickListEvent   pickListEvent   = objectMapper.readValue(pickListEventJson, PickListEvent.class);

        log.info("ProcessPickListEventDelegate executing for pickInstructionId: {}", pickInstructionId);

        String orderStatus = pickInstructionService.processPickListEvent(
                pickInstructionId, pickListEvent, pickInstruction);

        Map<String, Object> out = new HashMap<>();
        if ("released".equals(orderStatus)) {
            log.info("Order released for pickInstructionId: {} — triggering workflow completion", pickInstructionId);
            out.put("command", "COMPLETE");
        } else {
            out.put("command", "UPDATE");
        }
        return out;
    }
}
