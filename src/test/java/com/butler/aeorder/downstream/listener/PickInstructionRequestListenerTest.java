package com.butler.aeorder.downstream.listener;

import com.butler.aeorder.dto.PickInstructionRequestMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link PickInstructionRequestListener#validationError}.
 * Pure static logic — no mocks, no Spring context.
 */
class PickInstructionRequestListenerTest {

    private PickInstructionRequestMessage validMsg() {
        PickInstructionRequestMessage msg = new PickInstructionRequestMessage();
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
        PickInstructionRequestMessage msg = validMsg();
        msg.setId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_blank_id() {
        PickInstructionRequestMessage msg = validMsg();
        msg.setId("  ");
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_orderId() {
        PickInstructionRequestMessage msg = validMsg();
        msg.setOrderId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_orderlineId() {
        PickInstructionRequestMessage msg = validMsg();
        msg.setOrderlineId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_zero_qty() {
        PickInstructionRequestMessage msg = validMsg();
        msg.setQty(0);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }

    @Test
    void returns_error_for_null_slotId() {
        PickInstructionRequestMessage msg = validMsg();
        msg.setSlotId(null);
        assertNotNull(PickInstructionRequestListener.validationError(msg));
    }
}
