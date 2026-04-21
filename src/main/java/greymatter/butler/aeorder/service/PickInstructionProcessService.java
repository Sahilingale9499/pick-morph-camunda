package greymatter.butler.aeorder.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstruction;
import org.camunda.bpm.engine.ProcessEngineException;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Camunda process management service.
 * Replaces PickInstructionWorkflowStarter — starts and terminates
 * the "pickInstructionProcess" Camunda BPM process.
 */
@Service
@Slf4j
public class PickInstructionProcessService {

    private final RuntimeService runtimeService;
    private final ObjectMapper objectMapper;

    public PickInstructionProcessService(RuntimeService runtimeService,
                                          ObjectMapper objectMapper) {
        this.runtimeService = runtimeService;
        this.objectMapper = objectMapper;
    }

    /**
     * Start a new Camunda process instance from a PickInstruction (Kafka-triggered).
     * Idempotent: duplicate starts for the same pickId are silently ignored.
     */
    public void startProcess(PickInstruction msg) {
        String businessKey = "Order_workflow_" + msg.getId();
        try {
            String instructionJson = objectMapper.writeValueAsString(msg);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", msg.getId());
            vars.put("pickInstructionId", msg.getId());
            vars.put("orderId", msg.getOrderId());
            vars.put("orderlineId", msg.getOrderlineId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");
            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickId: {}", msg.getId());
        } catch (ProcessEngineException e) {
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            if (message.contains("unique") || message.contains("duplicate") || message.contains("already exists")) {
                log.warn("Process already running for pickId: {} — duplicate start suppressed", msg.getId());
                return;
            }
            log.error("Failed to start Camunda process for pickId: {}", msg.getId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + msg.getId(), e);
        } catch (Exception e) {
            log.error("Failed to start Camunda process for pickId: {}", msg.getId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + msg.getId(), e);
        }
    }


}
