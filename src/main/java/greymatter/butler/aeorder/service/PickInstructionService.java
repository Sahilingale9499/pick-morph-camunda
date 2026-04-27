package greymatter.butler.aeorder.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.aeorder.dto.AePickListRequest;
import greymatter.butler.aeorder.dto.ItemPickedEvent;
import greymatter.butler.aeorder.dto.OrderUpdateEvent;
import greymatter.butler.aeorder.dto.PickInstruction;
import greymatter.butler.aeorder.dto.ae.PickListEvent;
import greymatter.butler.aeorder.model.AeOrder;
import greymatter.butler.base.model.OrderMapping;
import greymatter.butler.base.model.Outbox;
import greymatter.butler.aeorder.model.TransactionStatus;
import greymatter.butler.aeorder.repository.AeOrderRepository;
import greymatter.butler.base.repository.OrderMappingRepository;
import greymatter.butler.base.repository.OutboxEventRepository;
import greymatter.butler.aeorder.repository.TransactionStatusRepository;
import greymatter.butler.base.service.OutboxService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Business logic service extracted from PickActivitiesImpl.
 * Used by Camunda JavaDelegate classes to perform all domain operations.
 */
@Service
@Slf4j
public class PickInstructionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionStatusRepository transactionStatusRepository;
    private final AeOrderRepository aeOrderRepository;
    private final OrderMappingRepository aeOrdersMappingRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final AeOrderBuilderService aeOrderBuilderService;
    private final AeOrderPersistenceService aeOrderPersistenceService;
    private final OutboxService outboxService;

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    @Value("${kafka.topic.order-update-events}")
    private String orderUpdateEventsTopic;

    @Value("${kafka.topics.pick-instruction-response}")
    private String pickInstructionResponseTopic;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  TransactionStatusRepository transactionStatusRepository,
                                  AeOrderRepository aeOrderRepository,
                                  OrderMappingRepository aeOrdersMappingRepository,
                                  OutboxEventRepository outboxEventRepository,
                                  ObjectMapper objectMapper,
                                  AeOrderBuilderService aeOrderBuilderService,
                                  AeOrderPersistenceService aeOrderPersistenceService,
                                  OutboxService outboxService) {
        this.kafkaTemplate = kafkaTemplate;
        this.transactionStatusRepository = transactionStatusRepository;
        this.aeOrderRepository = aeOrderRepository;
        this.aeOrdersMappingRepository = aeOrdersMappingRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.aeOrderBuilderService = aeOrderBuilderService;
        this.aeOrderPersistenceService = aeOrderPersistenceService;
        this.outboxService = outboxService;
    }

    // ─── Kafka-triggered workflow start ─────────────────────────────────────

    /**
     * Build AePickListRequest from the Kafka message, persist to ae_order table,
     * and publish to "pick-list.requests" via transactional outbox.
     */
    @Transactional
    public void publishPickListRequest(PickInstruction msg) {
        AePickListRequest request = aeOrderBuilderService.build(msg);
        aeOrderPersistenceService.saveAeOrder(request);
        outboxService.save(pickListRequestsTopic, msg.getId(), request, "pick_list_request");
        log.info("Persisted AePickListRequest and queued to pick-list.requests for pickId: {}", msg.getId());
    }

    /**
     * Atomically mark AE order as FAILED and publish failure response to pick-instruction.response.
     * Called on the BPMN validation-failure path.
     */
    @Transactional
    public void terminateWithFailureResponse(String pickId, String status, String orderId,
            String orderlineId, String message, String errorCode, String errorsJson) {
        aeOrderPersistenceService.markAsFailed(pickId);
        Map<String, Object> response = buildFailureResponseMap(
                pickId, status, orderId, orderlineId, message, errorCode, errorsJson, objectMapper);
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Marked AE order as FAILED and queued failure response | pickId: {}", pickId);
    }

    /**
     * Publish pick-instruction.response to notify butler_server of validation outcome.
     */
    @Transactional
    public void publishPickInstructionResponse(String pickId, String status,
            String orderId, String orderlineId, String message, String errorCode, String errorsJson) {
        Map<String, Object> response = buildPickInstructionResponseMap(
                pickId, status, orderId, orderlineId, message);
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Queued pick-instruction.response for pickId: {} | status: {}", pickId, status);
    }

    // ─── Step 3: Update ae_order from events ────────────────────────────────

    /**
     * Selectively updates structured ae_order columns when a pick-list.events event arrives.
     *
     * Two levels of update (null-safe — never clobbers existing values with null):
     *   1. Parent AeOrder: state, subState, actuals, expectations, attributes (JSONB merged)
     *   2. Child AeOrder rows (matched via ae_orders_mapping by externalServiceRequestId):
     *      state, subState, status, actuals, expectations, attributes (JSONB merged)
     *
     * Creates a minimal parent ae_order record if one doesn't exist yet (guards against
     * rare cases where the event arrives before the Camunda delegate has run).
     */
    public String updateAeOrderFromEvent(PickListEvent event) {
        String pickInstructionId = event.getPayload().getExternalServiceRequestId();
        String computedOrderStatus = "created";

        AeOrder parent = aeOrderRepository.findByExternalServiceRequestId(pickInstructionId)
                .orElseGet(() -> {
                    log.warn("ae_order not found for pickInstructionId: {} during event update — creating partial record", pickInstructionId);
                    return AeOrder.builder()
                            .externalServiceRequestId(pickInstructionId)
                            .type("PICK")
                            .state("CREATED")
                            .subState("CREATED")
                            .status("CREATED")
                            .actuals("{}")
                            .isDeleted(false)
                            .stages("[]")
                            .onHold(false)
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build();
                });

        try {
            PickListEvent.Payload evtPayload = event.getPayload();

            // ── Level 1: parent selective update ─────────────────────────────
            if (evtPayload.getState() != null) {
                parent.setState(evtPayload.getState());
            }
            if (evtPayload.getAttributes() != null && evtPayload.getAttributes().getSubState() != null) {
                parent.setSubState(evtPayload.getAttributes().getSubState());
            }
            if (evtPayload.getActuals() != null) {
                parent.setActuals(objectMapper.writeValueAsString(evtPayload.getActuals()));
            }
            if (evtPayload.getExpectations() != null) {
                parent.setExpectations(objectMapper.writeValueAsString(evtPayload.getExpectations()));
            }
            if (evtPayload.getAttributes() != null) {
                // Merge into existing attributes JSON — preserves original fields
                Map<String, Object> storedAttrs = parent.getAttributes() != null
                        ? objectMapper.readValue(parent.getAttributes(), new TypeReference<Map<String, Object>>() {})
                        : new LinkedHashMap<>();
                PickListEvent.PayloadAttributes ea = evtPayload.getAttributes();
                mergeIfNotNull(storedAttrs, "event_type",    ea.getEventType());
                mergeIfNotNull(storedAttrs, "sub_state",     ea.getSubState());
                mergeIfNotNull(storedAttrs, "cust_identity", ea.getCustIdentity());
                mergeIfNotNull(storedAttrs, "destination",   ea.getDestination());
                mergeIfNotNull(storedAttrs, "flow_name",     ea.getFlowName());
                parent.setAttributes(objectMapper.writeValueAsString(storedAttrs));
            }

            // ── Level 2: per-child AeOrder selective update ───────────────────
            List<OrderMapping> mappings = aeOrdersMappingRepository
                    .findByParentExternalServiceRequestId(pickInstructionId);
            List<Map<String, Object>> childStatusMaps = new ArrayList<>();

            for (OrderMapping mapping : mappings) {
                String childId = mapping.getChildExternalServiceRequestId();
                Optional<AeOrder> childOpt = aeOrderRepository.findByExternalServiceRequestId(childId);
                if (childOpt.isEmpty()) continue;
                AeOrder child = childOpt.get();

                // Apply matching event SR fields
                if (evtPayload.getServiceRequests() != null) {
                    evtPayload.getServiceRequests().stream()
                            .filter(sr -> childId.equals(sr.getExternalServiceRequestId()))
                            .findFirst()
                            .ifPresent(evtSR -> {
                                if (evtSR.getState() != null) child.setState(evtSR.getState());
                                if (evtSR.getAttributes() != null
                                        && evtSR.getAttributes().getSubState() != null) {
                                    child.setSubState(evtSR.getAttributes().getSubState());
                                }
                                try {
                                    if (evtSR.getActuals() != null) {
                                        child.setActuals(objectMapper.writeValueAsString(evtSR.getActuals()));
                                    }
                                    if (evtSR.getExpectations() != null) {
                                        child.setExpectations(objectMapper.writeValueAsString(evtSR.getExpectations()));
                                    }
                                    if (evtSR.getAttributes() != null) {
                                        Map<String, Object> storedSRAttrs = child.getAttributes() != null
                                                ? objectMapper.readValue(child.getAttributes(), new TypeReference<Map<String, Object>>() {})
                                                : new LinkedHashMap<>();
                                        PickListEvent.ServiceRequestAttributes sa = evtSR.getAttributes();
                                        mergeIfNotNull(storedSRAttrs, "sub_state",       sa.getSubState());
                                        mergeIfNotNull(storedSRAttrs, "orderType",        sa.getOrderType());
                                        mergeIfNotNull(storedSRAttrs, "simple_priority",  sa.getSimplePriority());
                                        child.setAttributes(objectMapper.writeValueAsString(storedSRAttrs));
                                    }
                                } catch (Exception ex) {
                                    log.warn("Failed to merge SR fields for child: {}", childId, ex);
                                }
                            });
                }

                // Compute OL status for this child and save
                try {
                    Map<String, Object> srMap = new LinkedHashMap<>();
                    srMap.put("actuals", child.getActuals() != null
                            ? objectMapper.readValue(child.getActuals(), new TypeReference<Map<String, Object>>() {}) : null);
                    srMap.put("expectations", child.getExpectations() != null
                            ? objectMapper.readValue(child.getExpectations(), new TypeReference<Map<String, Object>>() {}) : null);
                    String olStatus = AeOrderStatusCalculator.computeOlStatus(srMap);
                    child.setStatus(olStatus);
                    child.setUpdatedAt(Instant.now());
                    aeOrderRepository.save(child);
                    Map<String, Object> statusEntry = new LinkedHashMap<>();
                    statusEntry.put("status", olStatus);
                    childStatusMaps.add(statusEntry);
                } catch (Exception ex) {
                    log.warn("Failed to compute/set OL status for child: {}", childId, ex);
                }
            }

            // ── Level 3: derive aggregate order status from children ──────────
            computedOrderStatus = AeOrderStatusCalculator.computeOrderStatus(childStatusMaps);
            parent.setStatus(computedOrderStatus);
            parent.setUpdatedAt(Instant.now());
            aeOrderRepository.save(parent);
            log.info("Updated ae_order | pickInstructionId: {}, state: {}, orderStatus: {}",
                    pickInstructionId, evtPayload.getState(), computedOrderStatus);

        } catch (Exception e) {
            log.warn("Failed to update ae_order for pickInstructionId: {} — non-critical, continuing", pickInstructionId, e);
        }
        return computedOrderStatus;
    }

    // ─── Step 4: Process pick-list event (unified, event_type-agnostic) ────────

    /**
     * Unified handler for every pick-list event regardless of event_type.
     * @return derived order status (created | pending | complete | released)
     */
    @Transactional
    public String processPickListEvent(String pickInstructionId, PickListEvent event, PickInstruction pi) {
        // Step 1: always update ae_order
        String orderStatus = updateAeOrderFromEvent(event);

        PickListEvent.Payload payload = event.getPayload();
        if (payload != null && payload.getServiceRequests() != null) {
            String state    = payload.getState();
            String subState = payload.getAttributes() != null ? payload.getAttributes().getSubState() : null;

            boolean anyTransactions = false;
            boolean itemPickedDispatched = false;
            for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
                Set<String> processedTxIds = new HashSet<>();

                // Cases A & B — normal transactions[]
                if (sr.getTransactions() != null) {
                    for (PickListEvent.Transaction tx : sr.getTransactions()) {
                        anyTransactions = true;
                        String txId = tx.getTransactionId();

                        // Dedup: skip transactions already processed
                        if (txId != null && !txId.isEmpty()) {
                            Optional<TransactionStatus> existing = transactionStatusRepository.findById(txId);
                            if (existing.isPresent() && "SUCCESS".equals(existing.get().getStatus())) {
                                log.info("Duplicate txId: {} for pickInstructionId: {} — skipping", txId, pickInstructionId);
                                continue;
                            }
                            persistTransactionStatus(txId, pickInstructionId, "SUCCESS", tx);
                        }

                        String containerStatus = tx.getContainerAttributes() != null
                                ? tx.getContainerAttributes().getStatus() : null;
                        if (containerStatus == null) {
                            log.debug("Transaction {} has no containerAttributes.status — skipping dispatch", txId);
                            continue;
                        }

                        switch (containerStatus.toLowerCase()) {
                            case "complete" -> {
                                enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, "bot", sr.getExceptions());
                                itemPickedDispatched = true;
                                if (txId != null) processedTxIds.add(txId);
                            }
                            case "unloaded" -> {
                                enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, "undefined", sr.getExceptions());
                                itemPickedDispatched = true;
                                if (txId != null) processedTxIds.add(txId);
                            }
                            default -> log.debug(
                                    "Transaction {} status '{}' — SR-level order_update handled by enqueueOrderUpdate",
                                    txId, containerStatus);
                        }
                    }
                }

                // Case C — exception-only transactions (transactionId not in transactions[])
                // processedTxIds.add() returns false if already present → prevents duplicates
                if (sr.getExceptions() != null) {
                    for (PickListEvent.ExceptionItem ex : sr.getExceptions()) {
                        String exTxId = ex.getTransactionId();
                        if (exTxId != null && processedTxIds.add(exTxId)) {
                            try {
                                Long internalOrderId = ex.getContainerAttributes() != null
                                        ? ex.getContainerAttributes().getInternalOrderId() : null;
                                if (internalOrderId == null) {
                                    log.warn("No internalOrderId in exception containerAttributes for exTxId: {} — skipping", exTxId);
                                    continue;
                                }
                                String exItemPickedTxId = pickInstructionId + "_" + internalOrderId;
                                String exBotId = ex.getContainerAttributes().getBotId();
                                ItemPickedEvent.ExceptionInfo exceptionInfo = buildExceptionInfo(sr.getExceptions(), exTxId);
                                buildAndSaveItemPickedEvent(pickInstructionId, pi, exItemPickedTxId, null, 0, "bot", exceptionInfo, null, internalOrderId, null, exBotId);
                                log.info("Enqueued ItemPickedEvent (exception-only) | pickInstructionId: {} | txId: {} | exTxId: {} | exState: {}",
                                        pickInstructionId, exItemPickedTxId, exTxId, ex.getState());
                                itemPickedDispatched = true;
                                anyTransactions = true;
                            } catch (Exception e) {
                                log.warn("Failed to enqueue exception-only ItemPickedEvent for exTxId: {} — non-critical",
                                        exTxId, e);
                            }
                        }
                    }
                }
            }

            if (!anyTransactions) {
                log.debug("No transactions in event for pickInstructionId: {} — skipping transaction dispatch",
                        pickInstructionId);
            }

            // Skip order_update when item_picked.events were sent for this event
            if (itemPickedDispatched) {
                log.debug("item_picked.events dispatched for pickInstructionId: {} — suppressing order_update", pickInstructionId);
                return orderStatus;
            }
        }

        // Step 3: SR-level order update (only when no item_picked.events were sent)
        enqueueOrderUpdate(pickInstructionId, pi, event);

        return orderStatus;
    }

    /**
     * Builds and enqueues an {@link ItemPickedEvent} outbox entry for a single transaction.
     *
     * @param danglingArea "bot" when containerStatus is {@code complete}; "undefined" when containerStatus is {@code unloaded}.
     */
    private void enqueueItemPickedEventForTransaction(String pickInstructionId,
                                                      PickListEvent.Transaction tx,
                                                      PickInstruction pi,
                                                      String danglingArea,
                                                      List<PickListEvent.ExceptionItem> exceptions) {
        try {
            int  pickedQty       = tx.getContainerAttributes() != null ? tx.getContainerAttributes().getQtyPicked() : 0;
            Long internalOrderId = tx.getContainerAttributes() != null ? tx.getContainerAttributes().getInternalOrderId() : null;
            if (internalOrderId == null) {
                throw new IllegalStateException("internalOrderId is required but not present in containerAttributes");
            }
            String itemPickedTxId = pickInstructionId + "_" + internalOrderId;
            String toteId = tx.getContainerAttributes().getToteId();

            ItemPickedEvent.ExceptionInfo exceptionInfo = buildExceptionInfo(exceptions, tx.getTransactionId());

            buildAndSaveItemPickedEvent(pickInstructionId, pi, itemPickedTxId,
                    tx.getTransactionState(), pickedQty, danglingArea, exceptionInfo, toteId, internalOrderId,
                    tx.getContainerAttributes().getStatus(), tx.getContainerAttributes().getBotId());
            log.info("Enqueued ItemPickedEvent | pickInstructionId: {} | txId: {} | containerStatus: {} | danglingArea: {}",
                    pickInstructionId, tx.getTransactionId(),
                    tx.getContainerAttributes() != null ? tx.getContainerAttributes().getStatus() : "?",
                    danglingArea);
        } catch (Exception e) {
            log.warn("Failed to enqueue ItemPickedEvent for tx: {} pickInstructionId: {} — non-critical",
                    tx.getTransactionId(), pickInstructionId, e);
        }
    }

    static ItemPickedEvent.ExceptionInfo buildExceptionInfo(
            List<PickListEvent.ExceptionItem> exceptions, String transactionId) {
        if (exceptions == null || exceptions.isEmpty()) return null;

        int missing = 0, physicallyDamaged = 0;
        for (PickListEvent.ExceptionItem ex : exceptions) {
            if (!Objects.equals(transactionId, ex.getTransactionId())) continue;
            int qty = ex.getProducts() != null
                    ? ex.getProducts().stream()
                            .mapToInt(PickListEvent.ExceptionProduct::getProductQuantity).sum()
                    : 0;
            switch (ex.getState() != null ? ex.getState().toLowerCase() : "") {
                case "item_missing" -> missing += qty;
                case "item_damaged" -> physicallyDamaged += qty;
                default -> log.warn("Unsupported exception state: {}", ex.getState());
            }
        }

        if (missing == 0 && physicallyDamaged == 0) return null;

        return ItemPickedEvent.ExceptionInfo.builder()
                .missing(missing)
                .unscannable(0)
                .physicallyDamaged(physicallyDamaged)
                .checklistException(0)
                .build();
    }

    static Map<String, Object> buildFailureResponseMap(
            String pickId, String status, String orderId, String orderlineId,
            String message, String errorCode, String errorsJson, ObjectMapper objectMapper) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        response.put("orderline_id", orderlineId);
        response.put("errorCode", errorCode);
        List<?> errors = null;
        if (errorsJson != null) {
            try {
                errors = objectMapper.readValue(errorsJson, List.class);
            } catch (Exception e) {
                log.warn("Failed to parse errorsJson for pickId: {}", pickId, e);
            }
        }
        response.put("errors", errors);
        return response;
    }

    static Map<String, Object> buildPickInstructionResponseMap(
            String pickId, String status, String orderId, String orderlineId, String message) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        response.put("orderline_id", orderlineId);
        return response;
    }

    private void buildAndSaveItemPickedEvent(
            String pickInstructionId,
            PickInstruction pi,
            String transactionId,
            String transactionState,
            int pickedQty,
            String danglingArea,
            ItemPickedEvent.ExceptionInfo exceptionInfo,
            String toteId,
            Long internalOrderId,
            String containerStatus,
            String botId) {

        ItemPickedEvent.PickedItemInfo itemInfo = ItemPickedEvent.PickedItemInfo.builder()
                .tpid(pi.getTpid())
                .itemUid(pi.getItemId())
                .uom(pi.getUom())
                .pickedQty(pickedQty)
                .pickInstructionIds(List.of(pickInstructionId))
                .exception(exceptionInfo)
                .build();

        ItemPickedEvent evt = ItemPickedEvent.builder()
                .ppsId(pi.getPpsId())
                .orderId(pi.getOrderId())
                .slotRef(pi.getSlotId())
                .ppsBinId(pi.getBinId())
                .transactionId(transactionId)
                .state(transactionState)
                .danglingArea(danglingArea)
                .toteId(toteId)
                .botId(botId)
                .internalOrderId(internalOrderId)
                .status(containerStatus)
                .isMarkedContainerFlow(false)
                .pickedItemInfoList(List.of(itemInfo))
                .build();

        outboxService.save(itemPickedEventsTopic, pickInstructionId, evt, "item_picked");
    }

    // ─── Order update notifications ─────────────────────────────────────────

    /**
     * Publishes one {@link OrderUpdateEvent} per serviceRequest to the order_update.events topic
     * via the transactional outbox. Called for both update and pick_transaction events so
     * Butler Core can call make_and_send_order_related_notifications for its business orders.
     *
     * order_id / orderline_id come from the PickInstruction (customer order IDs),
     * not from the AE pick-list event.
     */
    @Transactional
    public void enqueueOrderUpdate(String pickInstructionId, PickInstruction pi, PickListEvent event) {
        PickListEvent.Payload payload = event.getPayload();
        if (payload == null || payload.getServiceRequests() == null) return;

        String state    = payload.getState();
        String subState = payload.getAttributes() != null ? payload.getAttributes().getSubState() : null;

        for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
            try {
                int allocatedQty = computeAllocatedQty(sr.getActuals());
                List<Object> transactionList = buildTransactionList(sr.getTransactions(), allocatedQty, pi.getQty(), pickInstructionId, pi.getPpsId(), pi.getItemId(), pi.getTpid(), pi.getSlotId());

                OrderUpdateEvent update = OrderUpdateEvent.builder()
                        .pickInstructionId(pickInstructionId)
                        .orderId(pi.getOrderId())
                        .orderlineId(pi.getOrderlineId())
                        .state(state)
                        .subState(subState)
                        .transactions(transactionList.isEmpty() ? null : transactionList)
                        .build();
                outboxService.save(orderUpdateEventsTopic, pickInstructionId, update, "update");
                log.info("Enqueued OrderUpdateEvent | pickInstructionId: {} | orderline: {} | state: {} | sub_state: {}",
                        pickInstructionId, sr.getExternalServiceRequestId(), state, subState);

                if ("fully_palletized".equals(subState)) {
                    Map<String, Object> deleteTransaction = new LinkedHashMap<>();
                    deleteTransaction.put("transaction_id", pickInstructionId);

                    OrderUpdateEvent deleteUpdate = OrderUpdateEvent.builder()
                            .pickInstructionId(pickInstructionId)
                            .orderId(pi.getOrderId())
                            .orderlineId(pi.getOrderlineId())
                            .state(state)
                            .subState(subState)
                            .transactions(List.of(deleteTransaction))
                            .build();
                    outboxService.save(orderUpdateEventsTopic, pickInstructionId, deleteUpdate, "delete");
                    log.info("Enqueued delete OrderUpdateEvent | pickInstructionId: {}", pickInstructionId);
                }
            } catch (Exception e) {
                log.warn("Failed to enqueue OrderUpdateEvent for pickInstructionId: {}, orderline: {} — non-critical",
                        pickInstructionId, sr.getExternalServiceRequestId(), e);
            }
        }
    }

    static int computeAllocatedQty(Object actuals) {
        if (!(actuals instanceof Map)) return 0;
        List<?> containers = (List<?>) ((Map<?, ?>) actuals).get("containers");
        if (containers == null) return 0;
        int total = 0;
        for (Object c : containers) {
            if (!(c instanceof Map)) continue;
            Object attrs = ((Map<?, ?>) c).get("containerAttributes");
            if (!(attrs instanceof Map)) continue;
            Object qty = ((Map<?, ?>) attrs).get("qty_to_be_picked");
            if (qty instanceof Number) total += ((Number) qty).intValue();
        }
        return total;
    }

    static List<Object> buildTransactionList(List<PickListEvent.Transaction> transactions,
                                               int allocatedQty,
                                               int totalQty,
                                               String pickInstructionId,
                                               int ppsId,
                                               String itemId,
                                               int tpid,
                                               String slotLocation) {
        List<Object> list = new ArrayList<>();
        if (transactions != null) {
            for (PickListEvent.Transaction tx : transactions) {
                PickListEvent.ContainerAttributes attrs = tx.getContainerAttributes();
                if (attrs == null) continue;
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("transaction_id", pickInstructionId + "_" + attrs.getInternalOrderId());
                entry.put("qty_to_be_picked", attrs.getQtyToBePicked());
                entry.put("qty_picked", attrs.getQtyPicked());
                entry.put("status", attrs.getStatus());
                entry.put("pps_id", attrs.getPpsId());
                entry.put("bot_id", attrs.getBotId());
                entry.put("internal_order_id", attrs.getInternalOrderId());
                entry.put("item_id", itemId);
                entry.put("tpid", tpid);
                if (attrs.getToteId() != null) {
                    entry.put("tote_id", attrs.getToteId());
                }
                if (attrs.getLocation() != null) {
                    entry.put("location", attrs.getLocation());
                }
                list.add(entry);
            }
        }
        int remainingQty = totalQty - allocatedQty;
        if (remainingQty > 0) {
            Map<String, Object> remainder = new LinkedHashMap<>();
            remainder.put("transaction_id", pickInstructionId);
            remainder.put("qty_to_be_picked", remainingQty);
            remainder.put("qty_picked", 0);
            remainder.put("status", "in_palletization");
            remainder.put("pps_id", ppsId);
            remainder.put("location", slotLocation);
            remainder.put("item_id", itemId);
            remainder.put("tpid", tpid);
            list.add(remainder);
        }
        return list;
    }

    private void persistTransactionStatus(String txId, String pickInstructionId, String status,
                                           PickListEvent.Transaction tx) {
        if (txId == null || txId.isEmpty()) {
            return;
        }
        try {
            String txPayload = null;
            if (tx != null) {
                try {
                    txPayload = objectMapper.writeValueAsString(tx);
                } catch (Exception e) {
                    log.warn("Could not serialize transaction payload for txId: {}", txId, e);
                }
            }
            TransactionStatus ts = new TransactionStatus(txId, pickInstructionId, status, Instant.now(), txPayload);
            transactionStatusRepository.save(ts);
        } catch (Exception e) {
            log.warn("Failed to persist transaction status {} for txId: {}", status, txId, e);
        }
    }

    /** Merges value into map only if value is non-null (never clobbers existing data). */
    private void mergeIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
