package greymatter.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.service.PickInstructionService;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Deserializes PickInstruction from the "instructionJson" process variable,
 * builds the AE order payload, persists it to PostgreSQL, and publishes to "pick-list.requests".
 */
@Component
@Slf4j
public class PublishOrderToKafkaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public PublishOrderToKafkaDelegate(PickInstructionService pickInstructionService,
                                       ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @JobWorker(type = "publish-order-to-kafka")
    public void publishOrder(@Variable String instructionJson) throws Exception {
        PickInstruction msg = objectMapper.readValue(instructionJson, PickInstruction.class);
        log.info("PublishOrderToKafkaDelegate executing for pickId: {}", msg.getId());
        pickInstructionService.publishPickListRequest(msg);
    }
}
