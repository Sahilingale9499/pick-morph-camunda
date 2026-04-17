package com.butler.aeorder.service;

import com.butler.aeorder.dto.ItemPickedEvent;
import com.butler.aeorder.dto.ae.PickListEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PickInstructionService#buildTransactionList}.
 * Pure static logic — no mocks, no Spring context.
 */
class PickInstructionServiceTest {

    // ── Constants ────────────────────────────────────────────────────────────

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PI_ID    = "PI-1";
    private static final int    PPS_ID   = 5;
    private static final String ITEM_ID  = "SKU-1";
    private static final int    TPID     = 42;
    private static final String SLOT_LOC = "SLOT-A";

    // ── Helpers ──────────────────────────────────────────────────────────────

    private PickListEvent.ContainerAttributes attrs(long internalOrderId,
                                                    String toteId,
                                                    String location) {
        return PickListEvent.ContainerAttributes.builder()
                .internalOrderId(internalOrderId)
                .qtyToBePicked(5)
                .qtyPicked(3)
                .status("loaded")
                .ppsId("PPS-1")
                .botId("BOT-1")
                .toteId(toteId)
                .location(location)
                .build();
    }

    private PickListEvent.Transaction tx(PickListEvent.ContainerAttributes attrs) {
        return PickListEvent.Transaction.builder()
                .containerAttributes(attrs)
                .build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> entry(List<Object> list, int index) {
        return (Map<String, Object>) list.get(index);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void returns_only_remainder_when_transactions_null() {
        List<Object> result = PickInstructionService.buildTransactionList(
                null, 3, 10, PI_ID, PPS_ID, ITEM_ID, TPID, SLOT_LOC);

        assertEquals(1, result.size());
        Map<String, Object> remainder = entry(result, 0);
        assertEquals(PI_ID, remainder.get("transaction_id"));
        assertEquals(7, remainder.get("qty_to_be_picked"));
        assertEquals(0, remainder.get("qty_picked"));
        assertEquals("in_palletization", remainder.get("status"));
        assertEquals(PPS_ID, remainder.get("pps_id"));
        assertEquals(SLOT_LOC, remainder.get("location"));
        assertEquals(ITEM_ID, remainder.get("item_id"));
        assertEquals(TPID, remainder.get("tpid"));
    }

    @Test
    void returns_only_remainder_when_transactions_empty() {
        List<Object> result = PickInstructionService.buildTransactionList(
                List.of(), 0, 5, PI_ID, PPS_ID, ITEM_ID, TPID, SLOT_LOC);

        assertEquals(1, result.size());
        Map<String, Object> remainder = entry(result, 0);
        assertEquals(5, remainder.get("qty_to_be_picked"));
        assertEquals("in_palletization", remainder.get("status"));
    }

    @Test
    void skips_transaction_when_container_attributes_are_null() {
        PickListEvent.Transaction txWithNullAttrs = PickListEvent.Transaction.builder()
                .containerAttributes(null)
                .build();

        List<Object> result = PickInstructionService.buildTransactionList(
                List.of(txWithNullAttrs), 0, 5, PI_ID, PPS_ID, ITEM_ID, TPID, SLOT_LOC);

        // tx entry is skipped; only the remainder entry is present
        assertEquals(1, result.size());
        assertEquals("in_palletization", entry(result, 0).get("status"));
    }

    @Test
    void appends_remainder_entry_when_qty_not_fully_allocated() {
        PickListEvent.ContainerAttributes a = attrs(101L, "TOTE-7", "LOC-5");
        List<Object> result = PickInstructionService.buildTransactionList(
                List.of(tx(a)), 3, 10, PI_ID, PPS_ID, ITEM_ID, TPID, SLOT_LOC);

        assertEquals(2, result.size());

        Map<String, Object> txEntry = entry(result, 0);
        assertEquals("PI-1_101", txEntry.get("transaction_id"));
        assertEquals("TOTE-7", txEntry.get("tote_id"));
        assertEquals("LOC-5", txEntry.get("location"));

        Map<String, Object> remainder = entry(result, 1);
        assertEquals(PI_ID, remainder.get("transaction_id"));
        assertEquals(7, remainder.get("qty_to_be_picked"));
        assertEquals("in_palletization", remainder.get("status"));
    }

    @Test
    void no_remainder_entry_when_all_qty_allocated() {
        PickListEvent.ContainerAttributes a = attrs(202L, "TOTE-9", "LOC-8");
        List<Object> result = PickInstructionService.buildTransactionList(
                List.of(tx(a)), 5, 5, PI_ID, PPS_ID, ITEM_ID, TPID, SLOT_LOC);

        // totalQty == allocatedQty → remainingQty = 0 → no remainder added
        assertEquals(1, result.size());
        Map<String, Object> txEntry = entry(result, 0);
        assertEquals("PI-1_202", txEntry.get("transaction_id"));
        assertEquals("TOTE-9", txEntry.get("tote_id"));
        assertEquals("LOC-8", txEntry.get("location"));
    }

    // ── buildExceptionInfo helpers ────────────────────────────────────────────

    private PickListEvent.ExceptionItem exceptionItem(String txId, String state, int... productQtys) {
        List<PickListEvent.ExceptionProduct> products = new ArrayList<>();
        for (int qty : productQtys) {
            products.add(PickListEvent.ExceptionProduct.builder().productQuantity(qty).build());
        }
        return PickListEvent.ExceptionItem.builder()
                .transactionId(txId)
                .state(state)
                .products(products.isEmpty() ? null : products)
                .build();
    }

    // ── buildExceptionInfo tests ──────────────────────────────────────────────

    @Test
    void returns_null_when_exceptions_is_null() {
        assertNull(PickInstructionService.buildExceptionInfo(null, "TX-1"));
    }

    @Test
    void returns_null_when_exceptions_is_empty() {
        assertNull(PickInstructionService.buildExceptionInfo(List.of(), "TX-1"));
    }

    @Test
    void returns_null_when_no_exception_matches_transaction_id() {
        List<PickListEvent.ExceptionItem> exceptions = List.of(
                exceptionItem("TX-other", "item_missing", 3));
        assertNull(PickInstructionService.buildExceptionInfo(exceptions, "TX-1"));
    }

    @Test
    void populates_both_missing_and_damaged_across_multiple_exceptions() {
        List<PickListEvent.ExceptionItem> exceptions = List.of(
                exceptionItem("TX-1", "item_missing", 3),
                exceptionItem("TX-1", "item_damaged", 2));
        ItemPickedEvent.ExceptionInfo info = PickInstructionService.buildExceptionInfo(exceptions, "TX-1");
        assertNotNull(info);
        assertEquals(3, info.getMissing());
        assertEquals(2, info.getPhysicallyDamaged());
        assertEquals(0, info.getUnscannable());
        assertEquals(0, info.getChecklistException());
    }

    @Test
    void returns_null_when_matching_exception_has_null_products() {
        // products=null → qty=0; both accumulators stay 0 → method returns null
        PickListEvent.ExceptionItem ex = PickListEvent.ExceptionItem.builder()
                .transactionId("TX-1")
                .state("item_missing")
                .products(null)
                .build();
        assertNull(PickInstructionService.buildExceptionInfo(List.of(ex), "TX-1"));
    }

    // ── buildFailureResponseMap tests ─────────────────────────────────────────

    @Test
    void failure_map_all_fields_present_with_correct_values() throws Exception {
        Map<String, Object> map = PickInstructionService.buildFailureResponseMap(
                "PI-1", "FAILURE", "ORD-1", "OL-1", "Validation failed",
                "ERR-400", "[\"e1\",\"e2\"]", MAPPER);

        assertEquals("PI-1", map.get("id"));
        assertEquals("ORD-1", map.get("order_id"));
        assertEquals("FAILURE", map.get("status"));
        assertEquals("Validation failed", map.get("message"));
        assertEquals("OL-1", map.get("orderline_id"));
        assertEquals("ERR-400", map.get("errorCode"));
        assertEquals(List.of("e1", "e2"), map.get("errors"));
    }

    @Test
    void failure_map_errors_null_when_errorsJson_is_null() {
        Map<String, Object> map = PickInstructionService.buildFailureResponseMap(
                "PI-1", "FAILURE", "ORD-1", "OL-1", "msg", "ERR-400", null, MAPPER);

        assertTrue(map.containsKey("errors"));
        assertNull(map.get("errors"));
    }

    @Test
    void failure_map_errors_null_when_errorsJson_malformed() {
        Map<String, Object> map = PickInstructionService.buildFailureResponseMap(
                "PI-1", "FAILURE", "ORD-1", "OL-1", "msg", "ERR-400", "not-valid-json", MAPPER);

        assertTrue(map.containsKey("errors"));
        assertNull(map.get("errors"));
    }

    // ── computeAllocatedQty tests ─────────────────────────────────────────────

    @Test
    void returns_zero_when_containers_list_is_empty() {
        Map<String, Object> actuals = new LinkedHashMap<>();
        actuals.put("containers", List.of());
        assertEquals(0, PickInstructionService.computeAllocatedQty(actuals));
    }

    @Test
    void sums_qty_across_multiple_containers() {
        Map<String, Object> attrs1 = Map.of("qty_to_be_picked", 3);
        Map<String, Object> c1 = Map.of("containerAttributes", attrs1);
        Map<String, Object> attrs2 = Map.of("qty_to_be_picked", 7);
        Map<String, Object> c2 = Map.of("containerAttributes", attrs2);
        Map<String, Object> actuals = Map.of("containers", List.of(c1, c2));
        assertEquals(10, PickInstructionService.computeAllocatedQty(actuals));
    }

    @Test
    void skips_container_without_containerAttributes() {
        Map<String, Object> c = new LinkedHashMap<>(); // no containerAttributes key
        Map<String, Object> actuals = Map.of("containers", List.of(c));
        assertEquals(0, PickInstructionService.computeAllocatedQty(actuals));
    }

    // ── buildPickInstructionResponseMap tests ─────────────────────────────────

    @Test
    void pi_response_all_fields_present_with_correct_values() {
        Map<String, Object> map = PickInstructionService.buildPickInstructionResponseMap(
                "PI-1", "SUCCESS", "ORD-1", "OL-1", "Accepted");

        assertEquals("PI-1", map.get("id"));
        assertEquals("ORD-1", map.get("order_id"));
        assertEquals("SUCCESS", map.get("status"));
        assertEquals("Accepted", map.get("message"));
        assertEquals("OL-1", map.get("orderline_id"));
        assertFalse(map.containsKey("errorCode"));
        assertFalse(map.containsKey("errors"));
    }
}
