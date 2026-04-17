package com.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.AePickListRequest;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.repository.AeOrderRepository;
import com.butler.aeorder.repository.AeOrdersMappingRepository;
import com.butler.aeorder.repository.OutboxEventRepository;
import com.butler.aeorder.repository.TransactionStatusRepository;
import com.butler.aeorder.service.AeOrderBuilderService;
import com.butler.aeorder.service.AeOrderPersistenceService;
import com.butler.aeorder.service.OutboxService;
import com.butler.aeorder.service.PickInstructionService;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link PublishOrderToKafkaDelegate} end-to-end with a real
 * {@link PickInstructionService} and mocked external dependencies.
 * The delegate's own ObjectMapper is real (needed for JSON deserialization).
 */
@ExtendWith(MockitoExtension.class)
class PublishOrderToKafkaDelegateTest {

    @Mock KafkaTemplate<String, Object> kafkaTemplate;
    @Mock TransactionStatusRepository transactionStatusRepository;
    @Mock AeOrderRepository aeOrderRepository;
    @Mock AeOrdersMappingRepository aeOrdersMappingRepository;
    @Mock OutboxEventRepository outboxEventRepository;
    @Mock ObjectMapper objectMapper;
    @Mock AeOrderBuilderService aeOrderBuilderService;
    @Mock AeOrderPersistenceService aeOrderPersistenceService;
    @Mock OutboxService outboxService;

    @Mock DelegateExecution execution;

    @InjectMocks PickInstructionService service;

    private PublishOrderToKafkaDelegate delegate;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "pickListRequestsTopic",       "pick-list.requests");
        ReflectionTestUtils.setField(service, "itemPickedEventsTopic",        "item-picked-events");
        ReflectionTestUtils.setField(service, "orderUpdateEventsTopic",       "order-update-events");
        ReflectionTestUtils.setField(service, "pickInstructionResponseTopic", "pick-instruction.response");
        // The delegate gets a real ObjectMapper for JSON deserialization
        delegate = new PublishOrderToKafkaDelegate(service, new ObjectMapper());
    }

    @Test
    void execute_deserializes_json_and_publishes_pick_list_request() throws Exception {
        PickInstructionRequestMessage msg = new PickInstructionRequestMessage();
        msg.setId("PI-1");
        String json = new ObjectMapper().writeValueAsString(msg);

        when(execution.getVariable("instructionJson")).thenReturn(json);

        AePickListRequest aeOrder = AePickListRequest.builder()
                .externalServiceRequestId("PI-1")
                .build();
        when(aeOrderBuilderService.build(any(PickInstructionRequestMessage.class))).thenReturn(aeOrder);

        delegate.execute(execution);

        verify(aeOrderPersistenceService).saveAeOrder(aeOrder);
        verify(outboxService).save("pick-list.requests", "PI-1", aeOrder, "pick_list_request");
    }
}
