package greymatter.butler.aeorder.downstream.listener;

import greymatter.butler.aeorder.dto.ae.PickListEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link PickListEventListener#resolveEventType}.
 * Pure static logic — no mocks, no Spring context.
 */
class PickListEventListenerTest {

    @Test
    void returns_context_event_type_when_present() {
        PickListEvent event = PickListEvent.builder()
                .context(PickListEvent.Context.builder().eventType("pick_transaction").build())
                .build();
        assertEquals("pick_transaction", PickListEventListener.resolveEventType(event));
    }

    @Test
    void returns_payload_attributes_event_type_when_context_is_null() {
        PickListEvent event = PickListEvent.builder()
                .context(null)
                .payload(PickListEvent.Payload.builder()
                        .attributes(PickListEvent.PayloadAttributes.builder().eventType("update").build())
                        .build())
                .build();
        assertEquals("update", PickListEventListener.resolveEventType(event));
    }

    @Test
    void returns_payload_attributes_event_type_when_context_event_type_is_null() {
        PickListEvent event = PickListEvent.builder()
                .context(PickListEvent.Context.builder().eventType(null).build())
                .payload(PickListEvent.Payload.builder()
                        .attributes(PickListEvent.PayloadAttributes.builder().eventType("update").build())
                        .build())
                .build();
        assertEquals("update", PickListEventListener.resolveEventType(event));
    }

    @Test
    void returns_unknown_when_no_event_type_available() {
        PickListEvent event = PickListEvent.builder()
                .context(null)
                .payload(PickListEvent.Payload.builder().attributes(null).build())
                .build();
        assertEquals("unknown", PickListEventListener.resolveEventType(event));
    }
}
