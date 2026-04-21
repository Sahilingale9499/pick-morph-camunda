package greymatter.butler.aeorder.service;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link AeOrderStatusCalculator}.
 * Pure static logic — no mocks, no Spring context.
 */
class AeOrderStatusCalculatorTest {

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Builds a minimal srMap with actuals.containers containing one container
     * whose containerAttributes has the given qty_picked, qty_to_be_picked, and status.
     * expectations.containers[0].products[0].productQuantity is set to expectationQty.
     */
    private Map<String, Object> buildSrMap(int expectationQty, int qtyPicked,
                                           int qtyToBePicked, String containerStatus) {
        Map<String, Object> containerAttrs = new LinkedHashMap<>();
        containerAttrs.put("qty_picked", qtyPicked);
        containerAttrs.put("qty_to_be_picked", qtyToBePicked);
        containerAttrs.put("status", containerStatus);

        Map<String, Object> container = new LinkedHashMap<>();
        container.put("containerAttributes", containerAttrs);

        Map<String, Object> actuals = new LinkedHashMap<>();
        actuals.put("containers", List.of(container));

        Map<String, Object> product = new LinkedHashMap<>();
        product.put("productQuantity", expectationQty);

        Map<String, Object> expContainer = new LinkedHashMap<>();
        expContainer.put("products", List.of(product));

        Map<String, Object> expectations = new LinkedHashMap<>();
        expectations.put("containers", List.of(expContainer));

        Map<String, Object> srMap = new LinkedHashMap<>();
        srMap.put("actuals", actuals);
        srMap.put("expectations", expectations);
        return srMap;
    }

    /** Builds a status entry map as fed into computeOrderStatus. */
    private Map<String, Object> statusEntry(String status) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", status);
        return m;
    }

    // ── computeOlStatus ──────────────────────────────────────────────────────

    @Test
    void olStatus_created_when_expectation_zero() {
        // expectations.containers[0].products[0].productQuantity = 0
        Map<String, Object> srMap = buildSrMap(0, 3, 5, "loaded");
        assertEquals("created", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_created_when_no_actuals_containers() {
        Map<String, Object> actuals = new LinkedHashMap<>();
        actuals.put("containers", Collections.emptyList());

        Map<String, Object> product = new LinkedHashMap<>();
        product.put("productQuantity", 5);
        Map<String, Object> expContainer = new LinkedHashMap<>();
        expContainer.put("products", List.of(product));
        Map<String, Object> expectations = new LinkedHashMap<>();
        expectations.put("containers", List.of(expContainer));

        Map<String, Object> srMap = new LinkedHashMap<>();
        srMap.put("actuals", actuals);
        srMap.put("expectations", expectations);

        assertEquals("created", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_created_when_qty_picked_zero() {
        // Container present but qty_picked = 0
        Map<String, Object> srMap = buildSrMap(5, 0, 5, "created");
        assertEquals("created", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_pending_when_partial_qty_picked() {
        // 3 of 5 picked
        Map<String, Object> srMap = buildSrMap(5, 3, 5, "loaded");
        assertEquals("pending", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_complete_when_all_picked_and_loaded() {
        // 5 of 5 picked, container status = loaded (still on bot/tote)
        Map<String, Object> srMap = buildSrMap(5, 5, 5, "loaded");
        assertEquals("complete", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_released_when_all_picked_and_unloaded() {
        // 5 of 5 picked, container status = unloaded (delivered to destination)
        Map<String, Object> srMap = buildSrMap(5, 5, 5, "unloaded");
        assertEquals("released", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    @Test
    void olStatus_complete_when_multiple_containers_one_loaded() {
        // 2 containers: first unloaded (3 picked), second loaded (2 picked) → total 5 of 5
        // Any loaded means "complete" even if others are unloaded
        Map<String, Object> attrs1 = new LinkedHashMap<>();
        attrs1.put("qty_picked", 3);
        attrs1.put("qty_to_be_picked", 3);
        attrs1.put("status", "unloaded");

        Map<String, Object> attrs2 = new LinkedHashMap<>();
        attrs2.put("qty_picked", 2);
        attrs2.put("qty_to_be_picked", 2);
        attrs2.put("status", "loaded");

        Map<String, Object> c1 = new LinkedHashMap<>();
        c1.put("containerAttributes", attrs1);
        Map<String, Object> c2 = new LinkedHashMap<>();
        c2.put("containerAttributes", attrs2);

        Map<String, Object> actuals = new LinkedHashMap<>();
        actuals.put("containers", List.of(c1, c2));

        Map<String, Object> product = new LinkedHashMap<>();
        product.put("productQuantity", 5);
        Map<String, Object> expContainer = new LinkedHashMap<>();
        expContainer.put("products", List.of(product));
        Map<String, Object> expectations = new LinkedHashMap<>();
        expectations.put("containers", List.of(expContainer));

        Map<String, Object> srMap = new LinkedHashMap<>();
        srMap.put("actuals", actuals);
        srMap.put("expectations", expectations);

        assertEquals("complete", AeOrderStatusCalculator.computeOlStatus(srMap));
    }

    // ── computeOrderStatus ───────────────────────────────────────────────────

    @Test
    void orderStatus_created_when_null_input() {
        assertEquals("created", AeOrderStatusCalculator.computeOrderStatus(null));
    }

    @Test
    void orderStatus_created_when_empty_input() {
        assertEquals("created", AeOrderStatusCalculator.computeOrderStatus(Collections.emptyList()));
    }

    @Test
    void orderStatus_created_when_all_created() {
        List<Map<String, Object>> statuses = List.of(
                statusEntry("created"),
                statusEntry("created")
        );
        assertEquals("created", AeOrderStatusCalculator.computeOrderStatus(statuses));
    }

    @Test
    void orderStatus_pending_when_any_pending() {
        // pending has highest priority — wins over complete
        List<Map<String, Object>> statuses = List.of(
                statusEntry("created"),
                statusEntry("pending"),
                statusEntry("complete")
        );
        assertEquals("pending", AeOrderStatusCalculator.computeOrderStatus(statuses));
    }

    @Test
    void orderStatus_complete_when_mix_complete_released() {
        // complete beats released (not all released → allReleased = false)
        List<Map<String, Object>> statuses = List.of(
                statusEntry("complete"),
                statusEntry("released")
        );
        assertEquals("complete", AeOrderStatusCalculator.computeOrderStatus(statuses));
    }

    @Test
    void orderStatus_released_when_all_released() {
        List<Map<String, Object>> statuses = List.of(
                statusEntry("released"),
                statusEntry("released")
        );
        assertEquals("released", AeOrderStatusCalculator.computeOrderStatus(statuses));
    }

    @Test
    void orderStatus_created_when_null_status_in_entry() {
        // null status in map → treated as "created" (defensive null path)
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("status", null);
        assertEquals("created", AeOrderStatusCalculator.computeOrderStatus(List.of(entry)));
    }
}
