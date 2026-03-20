package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstructionRequestMessage;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Camunda process management service.
 * Starts and terminates the "pickInstructionProcess" Camunda BPM process.
 * Accepts PickInstructionRequestMessage as the workflow input — used by both the
 * Kafka listener (pick-instruction.requests) and the REST controller.
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
     * Start a new Camunda process instance for the given PickInstructionRequestMessage.
     * Idempotent: uses try-catch on the actual start call to avoid TOCTOU race conditions
     * where two threads could both pass a pre-check and create duplicate processes.
     */
    public void startProcess(PickInstructionRequestMessage msg) {
        String businessKey = "Order_workflow_" + msg.getId();

        try {
            String instructionJson = objectMapper.writeValueAsString(msg);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", msg.getId());
            vars.put("orderId", msg.getOrderId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");

            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickId: {}", msg.getId());
        } catch (ProcessEngineException e) {
            // Camunda throws when a process with the same business key already exists
            // (or on other engine-level conflicts). Treat as idempotent duplicate.
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

    /**
     * Terminate the running process instance for the given pickId, if one exists.
     */
    public void terminateProcess(String pickId) {
        String businessKey = "Order_workflow_" + pickId;
        ProcessInstance pi = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(businessKey)
                .singleResult();
        if (pi != null) {
            runtimeService.deleteProcessInstance(pi.getId(), "Manually terminated");
            log.info("Terminated Camunda process for pickId: {}", pickId);
        } else {
            log.warn("No running process found to terminate for pickId: {}", pickId);
        }
    }
}
