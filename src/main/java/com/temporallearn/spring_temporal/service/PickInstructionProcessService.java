package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
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
     * Start a new Camunda process instance for the given PickInstruction.
     * Idempotent: if a process with the same business key is already running,
     * the start is skipped and a warning is logged.
     */
    public void startProcess(PickInstruction instruction) {
        String businessKey = "Order_workflow_" + instruction.getPickId();

        // Idempotency check
        long existing = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(businessKey)
                .count();
        if (existing > 0) {
            log.warn("Process already running for pickId: {}", instruction.getPickId());
            return;
        }

        try {
            String instructionJson = objectMapper.writeValueAsString(instruction);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", instruction.getPickId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");

            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickId: {}", instruction.getPickId());
        } catch (Exception e) {
            log.error("Failed to start Camunda process for pickId: {}", instruction.getPickId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + instruction.getPickId(), e);
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
