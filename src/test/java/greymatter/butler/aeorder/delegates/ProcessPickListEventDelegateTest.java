package greymatter.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.dto.ae.PickListEvent;
import greymatter.butler.aeorder.service.PickInstructionService;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ProcessPickListEventDelegate}:
 *   - reads the correct execution variables and delegates to
 *     {@link PickInstructionService#processPickListEvent}
 *   - sets {@code command=COMPLETE} only when the service returns "released"
 *   - always sets {@code txStatus=SUCCESS} and {@code txComplete=false}
 */
@ExtendWith(MockitoExtension.class)
class ProcessPickListEventDelegateTest {

    @Mock PickInstructionService pickInstructionService;
    @Mock ObjectMapper objectMapper;
    @Mock DelegateExecution execution;

    private ProcessPickListEventDelegate delegate;

    private static final String PICK_INSTRUCTION_ID  = "PI-42";
    private static final String INSTRUCTION_JSON     = "{\"id\":\"PI-42\"}";
    private static final String PICK_LIST_EVENT_JSON = "{\"payload\":{}}";

    @BeforeEach
    void setUp() {
        delegate = new ProcessPickListEventDelegate(pickInstructionService, objectMapper);
    }

    @Test
    void execute_delegates_to_service_and_sets_tx_variables() throws Exception {
        PickInstruction pi    = new PickInstruction();
        PickListEvent   event = new PickListEvent();

        when(execution.getVariable("pickInstructionId")).thenReturn(PICK_INSTRUCTION_ID);
        when(execution.getVariable("instructionJson")).thenReturn(INSTRUCTION_JSON);
        when(execution.getVariable("pickListEventJson")).thenReturn(PICK_LIST_EVENT_JSON);
        when(objectMapper.readValue(INSTRUCTION_JSON,     PickInstruction.class)).thenReturn(pi);
        when(objectMapper.readValue(PICK_LIST_EVENT_JSON, PickListEvent.class)).thenReturn(event);
        when(pickInstructionService.processPickListEvent(PICK_INSTRUCTION_ID, event, pi))
                .thenReturn("created");

        delegate.execute(execution);

        verify(pickInstructionService).processPickListEvent(PICK_INSTRUCTION_ID, event, pi);
        verify(execution).setVariable("txStatus", "SUCCESS");
        verify(execution).setVariable("txComplete", false);
        verify(execution, never()).setVariable(eq("command"), any());
    }

    @Test
    void execute_sets_command_COMPLETE_when_order_is_released() throws Exception {
        PickInstruction pi    = new PickInstruction();
        PickListEvent   event = new PickListEvent();

        when(execution.getVariable("pickInstructionId")).thenReturn(PICK_INSTRUCTION_ID);
        when(execution.getVariable("instructionJson")).thenReturn(INSTRUCTION_JSON);
        when(execution.getVariable("pickListEventJson")).thenReturn(PICK_LIST_EVENT_JSON);
        when(objectMapper.readValue(INSTRUCTION_JSON,     PickInstruction.class)).thenReturn(pi);
        when(objectMapper.readValue(PICK_LIST_EVENT_JSON, PickListEvent.class)).thenReturn(event);
        when(pickInstructionService.processPickListEvent(PICK_INSTRUCTION_ID, event, pi))
                .thenReturn("released");

        delegate.execute(execution);

        verify(execution).setVariable("command", "COMPLETE");
        verify(execution).setVariable("txStatus", "SUCCESS");
        verify(execution).setVariable("txComplete", false);
    }
}
