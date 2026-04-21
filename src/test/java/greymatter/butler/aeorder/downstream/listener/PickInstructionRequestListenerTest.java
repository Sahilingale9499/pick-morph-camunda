package greymatter.butler.aeorder.downstream.listener;

import greymatter.butler.aeorder.dto.PickInstruction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link PickInstructionRequestListener#validationError}.
 * Pure static logic — no mocks, no Spring context.
 */
class PickInstructionRequestListenerTest {

    private PickInstruction validMsg() {
        PickInstruction msg = new PickInstruction();
        msg.setId("PI-1");
        msg.setOrderId("ORD-1");
        msg.setOrderlineId("OL-1");
        msg.setQty(5);
        msg.setSlotId("SLOT-1");
        return msg;
    }

    @Test
    void returns_null_for_valid_message() {
        assertNull(PickInstructionRequestListener.validationError(validMsg()));
    }

    @Test
    void returns_error_for_null_id() {
        PickInstruction msg = validMsg();
        msg.setId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_blank_id() {
        PickInstruction msg = validMsg();
        msg.setId("  ");
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_orderId() {
        PickInstruction msg = validMsg();
        msg.setOrderId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_orderlineId() {
        PickInstruction msg = validMsg();
        msg.setOrderlineId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_zero_qty() {
        PickInstruction msg = validMsg();
        msg.setQty(0);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_slotId() {
        PickInstruction msg = validMsg();
        msg.setSlotId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }
}
