package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstructionRequestMessage;
import lombok.extern.slf4j.Slf4j;
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
     * Idempotent: if a process with the same business key is already running,
     * the start is skipped and a warning is logged.
     */
    public void startProcess(PickInstructionRequestMessage msg) {
        String businessKey = "Order_workflow_" + msg.getId();

        // Idempotency check
        long existing = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(businessKey)
                .count();
        if (existing > 0) {
            log.warn("Process already running for pickId: {}", msg.getId());
            return;
        }

        try {
            String instructionJson = objectMapper.writeValueAsString(msg);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", msg.getId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");

            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickId: {}", msg.getId());
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
