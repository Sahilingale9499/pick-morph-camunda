package greymatter.butler.aeorder.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.repository.AeOrderRepository;
import io.camunda.client.CamundaClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Camunda 8 process management service.
 * Starts the "pickInstructionProcess" Zeebe workflow instance.
 */
@Service
@Slf4j
public class PickInstructionProcessService {

    private final CamundaClient camundaClient;
    private final AeOrderRepository aeOrderRepository;
    private final ObjectMapper objectMapper;

    public PickInstructionProcessService(CamundaClient camundaClient,
                                          AeOrderRepository aeOrderRepository,
                                          ObjectMapper objectMapper) {
        this.camundaClient = camundaClient;
        this.aeOrderRepository = aeOrderRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Start a new Zeebe process instance from a PickInstruction (Kafka-triggered).
     * Idempotent: duplicate starts for the same pickId are silently ignored.
     */
    public void startProcess(PickInstruction msg) {
        if (aeOrderRepository.findByExternalServiceRequestId(msg.getId()).isPresent()) {
            log.warn("Process already running for pickId: {} — duplicate start suppressed", msg.getId());
            return;
        }
        try {
            String instructionJson = objectMapper.writeValueAsString(msg);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", msg.getId());
            vars.put("pickInstructionId", msg.getId());
            vars.put("orderId", msg.getOrderId());
            vars.put("orderlineId", msg.getOrderlineId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");

            camundaClient.newCreateInstanceCommand()
                    .bpmnProcessId("pickInstructionProcess")
                    .latestVersion()
                    .variables(vars)
                    .send()
                    .join();

            log.info("Started Zeebe process instance for pickId: {}", msg.getId());
        } catch (Exception e) {
            log.error("Failed to start Zeebe process for pickId: {}", msg.getId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + msg.getId(), e);
        }
    }
}
