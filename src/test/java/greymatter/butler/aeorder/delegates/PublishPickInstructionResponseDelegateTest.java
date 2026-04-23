package greymatter.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.repository.AeOrderRepository;
import greymatter.butler.base.repository.OrderMappingRepository;
import greymatter.butler.base.repository.OutboxEventRepository;
import greymatter.butler.aeorder.repository.TransactionStatusRepository;
import greymatter.butler.aeorder.service.AeOrderBuilderService;
import greymatter.butler.aeorder.service.AeOrderPersistenceService;
import greymatter.butler.base.service.OutboxService;
import greymatter.butler.aeorder.service.PickInstructionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link PublishPickInstructionResponseDelegate} end-to-end with a real
 * {@link PickInstructionService} and mocked external dependencies.
 */
@ExtendWith(MockitoExtension.class)
class PublishPickInstructionResponseDelegateTest {

    @Mock KafkaTemplate<String, Object> kafkaTemplate;
    @Mock TransactionStatusRepository transactionStatusRepository;
    @Mock AeOrderRepository aeOrderRepository;
    @Mock OrderMappingRepository orderMappingRepository;
    @Mock OutboxEventRepository outboxEventRepository;
    @Mock ObjectMapper objectMapper;
    @Mock AeOrderBuilderService aeOrderBuilderService;
    @Mock AeOrderPersistenceService aeOrderPersistenceService;
    @Mock OutboxService outboxService;

    @InjectMocks PickInstructionService service;

    private PublishPickInstructionResponseDelegate delegate;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "pickListRequestsTopic",       "pick-list.requests");
        ReflectionTestUtils.setField(service, "itemPickedEventsTopic",        "item-picked-events");
        ReflectionTestUtils.setField(service, "orderUpdateEventsTopic",       "order-update-events");
        ReflectionTestUtils.setField(service, "pickInstructionResponseTopic", "pick-instruction.response");
        delegate = new PublishPickInstructionResponseDelegate(service);
    }

    @Test
    void publishResponse_enqueues_pick_instruction_response_to_outbox() throws Exception {
        delegate.publishResponse(
                "PI-1", "SUCCESS", "ORD-1", "OL-1",
                "OK", null, null);

        verify(outboxService).save(
                eq("pick-instruction.response"), eq("PI-1"), any(), eq("pick_instruction_response"));
    }
}
