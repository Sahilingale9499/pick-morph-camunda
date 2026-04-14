# Pick-Morph-Camunda — Butler Core Integration Test Guide

Tests the full pick instruction workflow using **real butler_core payloads** captured from the
live integration run on 2026-04-01.

> **IMPORTANT — For AI assistants executing this guide:**
> Before executing any step (including Step 0 / setup), **always pause and ask the user for confirmation**.
> Show what you are about to run and wait for an explicit "yes" / "go ahead" before proceeding.
> Never run multiple steps back-to-back without user approval between each one.

---

## Test Data Reference

| Field | Value | Source |
|-------|-------|--------|
| `pickInstructionId` | `d86a1c62-ac3e-4c1b-880b-439cfbebaac2` | `id` from `gor.pick-instruction.requests` |
| `orderlineId` | `0` | `orderline_id` from `gor.pick-instruction.requests` |
| `order_id` | `2000` | `order_id` from `gor.pick-instruction.requests` |
| `qty` | `10` | `qty` from `gor.pick-instruction.requests` |
| `pps_id / bot_id` | `13` | `pps_id` from `gor.pick-instruction.requests` |
| `slot_id / location` | `ToteCant3.0.A.01` | `slot_id` from `gor.pick-instruction.requests` |
| `bin_id` | `2` | `bin_id` from `gor.pick-instruction.requests` |
| `tpid` | `39` | `tpid` from `gor.pick-instruction.requests` |
| `item_id` | `101722` | `item_id` from `gor.pick-instruction.requests` |
| `transactionId` | `3000000001` (3a-3), `3000000002` (3a-4), `3000000003` (3a-6), `3000000004` (3b), `3000000005` (4c) | `transactions[].transactionId` — unique per step |
| `internal_order_id` | `24234242` | `containerAttributes.internal_order_id` |
| `product_sku` | `bulk_20260318_114217_074972` | `productAttributes.product_sku` from `gor.pick-list.requests` |
| `product_uid` | `101722` | same as `item_id` |
| `user_name` | `grey7` | `containerAttributes.user_name` |

---

## 0. Setup / Clean Start

```bash
./setup.sh
```

### What it does

| Scenario | Actions |
|----------|---------|
| **spring-camunda is running** (hot) | ① Truncate `ae_order`, `transaction_status`, `outbox_event` <br>② Truncate Camunda runtime/history tables (`act_ru_*`, `act_hi_*`) <br>③ Delete Kafka topics <br>④ Rebuild & restart **only** `spring-camunda` (postgres/kafka untouched) |
| **Nothing is running** (cold) | `docker compose down -v` → `docker compose up --build -d` |

> The script waits for the health check to pass before exiting — safe to immediately run Step 1 once it completes.

---

## Step 1 — Start Workflow (pick instruction received)

Pick instructions can be triggered via **Kafka** (`gor.pick-instruction.requests`) or **REST** (kept for testing).

**Option A — Kafka:**

```bash
echo '{"source_service":"pick","timestamp":"2026-04-01T14:17:06Z","message_id":"msg_real_001","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"pick_instruction_created","payload":{"id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","order_id":"2000","orderline_id":"0","item_id":"101722","qty":10,"slot_id":"ToteCant3.0.A.01","uom":"Item","tpid":39,"pps_id":13,"bin_id":"2","barcodes":[]}}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-instruction.requests
```

**Option B — REST (testing convenience):**

```bash
curl -s -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{
    "id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
    "order_id": "2000",
    "orderline_id": "0",
    "item_id": "101722",
    "qty": 10,
    "slot_id": "ToteCant3.0.A.01",
    "uom": "Item",
    "tpid": 39,
    "pps_id": 13,
    "bin_id": "2",
    "barcodes": []
  }'
```

**Verify:**

```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 15s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Persisted ae_order|outbox|PUBLISHED"
# Expected:
#   "Pick instruction process started for pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2"
#   "Persisted ae_order + outbox event for pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2"
#   "Outbox PUBLISHED: ... topic=gor.pick-list.requests aggregateId=d86a1c62-ac3e-4c1b-880b-439cfbebaac2"

# 1. ae_order rows created (parent PICK + child PICK_LINE)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT external_service_request_id, type, state, sub_state, status, created_at
   FROM ae_order ORDER BY created_at;"
# Expected: two rows —
#   d86a1c62-ac3e-4c1b-880b-439cfbebaac2   | PICK      | CREATED | CREATED | CREATED
#   0                                        | PICK_LINE | CREATED | CREATED | CREATED

# 1b. mapping row links parent to child
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT parent_external_service_request_id, child_external_service_request_id
   FROM ae_orders_mapping;"
# Expected: parent=d86a1c62-ac3e-4c1b-880b-439cfbebaac2, child=0

# 2. outbox_event for pick-list.requests created and published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at DESC LIMIT 3;"
# Expected: topic=gor.pick-list.requests, status=PUBLISHED

# 3. Camunda process instance started
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 1;"
# Expected: business_key_=d86a1c62-ac3e-4c1b-880b-439cfbebaac2, end_time_=null (still running)
```

---

## Step 2 — AE Accepts Pick Request

**Topic:** `gor.pick-list.response`

```bash
echo '{"source_service":"srms","timestamp":"2026-04-01T14:18:00Z","message_id":"msg_real_002","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"pick_list_response","payload":{"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequestId":934622318,"executionId":"exec_real_001","status":"SUCCESS","message":"Order created successfully in SRMS","serviceRequests":[{"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequestId":0}],"timestamp":"2026-04-01T14:18:00.000Z"},"context":{"execution_id":"0"}}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Failure scenario (AE rejects — workflow aborts):**

```bash
echo '{"source_service":"srms","timestamp":"2026-04-01T14:18:00Z","message_id":"msg_real_002f","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"pick_list_response","payload":{"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequestId":null,"executionId":null,"status":"FAILURE","errorCode":"VALIDATION_FAILED","message":"ServiceRequest validation failed","errors":[{"field":"serviceRequests","code":"MISSING_ORDER_LINES","message":"ServiceRequests array is required and cannot be empty"}],"timestamp":"2026-04-01T14:18:00.000Z"},"context":{"execution_id":"0"}}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Verify:**

```bash
# Camunda message correlated in logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | grep -i "d86a1c62-ac3e-4c1b-880b-439cfbebaac2"
# Expect: "PickListResponseMessage correlated successfully"

# Camunda process still running (not ended)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, end_time_
   FROM act_hi_procinst WHERE business_key_='d86a1c62-ac3e-4c1b-880b-439cfbebaac2';"
# Expected: end_time_=null
```

---

## Step 2b — Pick Instruction Response to butler_server

> **LLD Step-4.** Once AE validates the pick-list.requests, the system publishes a
> `pick_instruction_response` back to butler_server on `gor.pick-instruction.response`
> via the transactional outbox (`PublishPickInstructionResponseDelegate`).

This step happens automatically — no manual trigger required. Verify it was published.

**Kafka topic (outbound):** `gor.pick-instruction.response`

**SUCCESS envelope (AE accepted)**

```json
{
  "name": "pick_instruction_response",
  "context": { "execution_id": "0" },
  "payload": {
    "id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
    "status": "SUCCESS",
    "message": "Order created successfully in SRMS",
    "order_id": "934622317",
    "orderline_id": "0"
  },
  "entity_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
  "timestamp": "<iso-timestamp>",
  "message_id": "<uuid>",
  "source_service": "ae-order-service"
}
```

**FAILURE envelope (AE rejected)**

```json
{
  "name": "pick_instruction_response",
  "context": { "execution_id": "0" },
  "payload": {
    "id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
    "status": "FAILURE",
    "message": "Validation failed",
    "order_id": null,
    "orderline_id": "0",
    "errorCode": "VALIDATION_FAILED",
    "errors": [
      {
        "code": "MISSING_ORDER_LINES",
        "field": "serviceRequests",
        "message": "Required"
      }
    ]
  },
  "entity_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
  "timestamp": "<iso-timestamp>",
  "message_id": "<uuid>",
  "source_service": "ae-order-service"
}
```

**Verify:**

```bash
# outbox_event published for pick-instruction.response
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status, published_at
   FROM outbox_event
   WHERE topic LIKE '%pick-instruction.response%'
   ORDER BY created_at DESC LIMIT 3;"
# Expected: aggregate_id=d86a1c62-ac3e-4c1b-880b-439cfbebaac2, status=PUBLISHED

# Logs — delegate fired
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "pick_instruction_response|PublishPickInstructionResponse|Queued pick-instruction.response"
# Expected: "Queued pick-instruction.response via outbox | pickId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | status: SUCCESS"
```

---

## Step 3a — Intermediate Order Update Events

> All intermediate events are `event_type=update` on topic `gor.pick-list.events`.
> Send them in order before Step 3b (pick_transaction).

### 3a-1 · cancellation_locked (sub_state=created)

When the order can no longer be cancelled. sub_state remains `created`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:30:00.000Z","message_id":"msg_real_3a1","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":12761,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":12762,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"101722","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = 'bulk_20260318_114217_074972'"],"package_count":10,"product_sku":"bulk_20260318_114217_074972","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"CREATED","state":"created","attributes":{"sub_state":"created","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant3.0.A.01","fullAddress":"ToteCant3.0.A.01","addressFields":{"slot_id":"ToteCant3.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"created","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|ProcessPickListEventDelegate|Updated ae_order|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | event_type: update | state: cancellation_locked | sub_state: created"
#   "Updated ae_order for pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2, state: cancellation_locked"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: cancellation_locked | sub_state: created"

docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          updated_at FROM ae_order WHERE external_service_request_id='d86a1c62-ac3e-4c1b-880b-439cfbebaac2';"
# Expected: state=cancellation_locked, sub_state=created
```

---

### 3a-2 · in_palletization

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-06T14:31:00.000Z","message_id":"msg_real_3a2","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-06T14:17:06.000Z","updatedOn":"2026-04-06T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-06T14:17:06.000Z","status":"CREATED","state":"created","attributes":{"sub_state":"in_palletization","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-06T14:17:06.000Z","updatedOn":"2026-04-06T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-06T14:17:06.000Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"in_palletization","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-06T14:17:06.000Z","updatedOn":"2026-04-06T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Updated ae_order|Enqueued OrderUpdateEvent"
# Expected:
#   "Updated ae_order for pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2, state: cancellation_locked"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: cancellation_locked | sub_state: in_palletization"
```

---

### 3a-3 · partially_palletized

`txId=3000000001` first seen. `qty_to_be_picked=1`, `qty_picked=0`, `status=created`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:32:00.000Z","message_id":"msg_real_3a3","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":12761,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":12762,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"3000000001","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","exceptions":[]}}]},"transactions":[{"transactionId":"3000000001","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","location":"ToteCant4.0.A.01","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"CREATED","state":"created","attributes":{"sub_state":"partially_palletized","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"partially_palletized","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Updated ae_order|Enqueued OrderUpdateEvent"
# Expected:
#   "Enqueued OrderUpdateEvent (created container) | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | txId: 3000000001 | orderline: 0"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: cancellation_locked | sub_state: partially_palletized"

docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at FROM ae_order WHERE external_service_request_id='d86a1c62-ac3e-4c1b-880b-439cfbebaac2';"
# Expected: state=cancellation_locked, sub_state=partially_palletized, container_status=created, qty_picked=0
```

---

### 3a-4 · fully_palletized

`txId=3000000002`. `qty_to_be_picked=10`, `qty_picked=0`, `status=created`.

```bash
docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events <<'KAFKA_EOF'
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:33:00.000Z","message_id":"msg_real_3a4","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":12761,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":12762,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"3000000001","containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","exceptions":[]}},{"transactionId":"3000000002","containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","exceptions":[]}}]},"transactions":[{"transactionId":"3000000002","containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","location":"ToteCant4.0.A.01","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"CREATED","state":"created","attributes":{"sub_state":"fully_palletized","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"fully_palletized","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Enqueued OrderUpdateEvent|Enqueued delete"
# Expected:
#   "Enqueued OrderUpdateEvent (created container) | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | txId: 3000000002 | orderline: 0"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: cancellation_locked | sub_state: fully_palletized"
#   "Enqueued delete OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2"
#
# Two messages published to gor.order_update.events:
#   1. name="update" — container transaction (transaction_id=3000000002, qty_to_be_picked=2, ...)
#   2. name="delete" — transactions=[{"transaction_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2"}]
```

---

### 3a-5 · internal_order_created

SR state transitions to `fulfillable`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:34:00.000Z","message_id":"msg_real_3a5","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":12761,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":12762,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"3000000002","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","exceptions":[]}}]},"transactions":[{"transactionId":"3000000002","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"created","bot_id":"13","pps_id":"13","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"101722","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = 'bulk_20260318_114217_074972'"],"package_count":10,"product_sku":"bulk_20260318_114217_074972","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"CREATED","state":"fulfillable","attributes":{"sub_state":"internal_order_created","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant3.0.A.01","fullAddress":"ToteCant3.0.A.01","addressFields":{"slot_id":"ToteCant3.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"internal_order_created","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Enqueued OrderUpdateEvent"
# Expected:
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: cancellation_locked | sub_state: internal_order_created"
```

---

### 3a-6 · fullfillable / in_progress (pallet loaded)

`txId=3000000003`. `status=loaded` → `OrderUpdateEvent` only (no `ItemPickedEvent`).

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:35:00.000Z","message_id":"msg_real_3a6","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":12761,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":12762,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"3000000003","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"loaded","bot_id":"13","pps_id":"13","exceptions":[]}}]},"transactions":[{"transactionId":"3000000003","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"loaded","bot_id":"13","pps_id":"13","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"101722","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = 'bulk_20260318_114217_074972'"],"package_count":10,"product_sku":"bulk_20260318_114217_074972","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"CREATED","state":"fulfillable","attributes":{"sub_state":"in_progress","orderType":"GCUS","sr_parentsIds":[12761],"has_parent":true,"location":{"displayName":"ToteCant3.0.A.01","fullAddress":"ToteCant3.0.A.01","addressFields":{"slot_id":"ToteCant3.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","state":"fulfillable","attributes":{"event_type":"update","sub_state":"in_progress","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**
```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62-ac3e-4c1b-880b-439cfbebaac2|Enqueued OrderUpdateEvent|Enqueued ItemPickedEvent"
# Expected:
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-ac3e-4c1b-880b-439cfbebaac2 | orderline: 0 | state: fulfillable | sub_state: in_progress"
#   NOTE: status=loaded → OrderUpdateEvent only, NO ItemPickedEvent
```

---

## Step 3b — Bot Picks Item (pick_transaction event)

**What happens:** AE publishes `pick_transaction`. Both containers picked — `3000000003` and `3000000004`, each `status="complete"`, `qty=1`.
Both txIds are new → **two `ItemPickedEvent`s enqueued, each with `danglingArea="bot"` and `qty_picked=1`** (pre-drop path).

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:36:00.000Z","message_id":"msg_real_3b","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null},{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000004","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null},{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000004","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[{"id":11648,"orderId":934622318,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSING","state":"pick_transaction","attributes":{"event_type":"pick_transaction","sub_state":"in_progress","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"pick_transaction"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62|Updated ae_order|Enqueued ItemPickedEvent|Enqueued OrderUpdateEvent"
# Expected:
#   "Updated ae_order for pickInstructionId: d86a1c62-..., state: pick_transaction"
#   "Enqueued ItemPickedEvent | pickInstructionId: d86a1c62-... | txId: 3000000003 | containerStatus: complete | danglingArea: bot"
#   "Enqueued ItemPickedEvent | pickInstructionId: d86a1c62-... | txId: 3000000004 | containerStatus: complete | danglingArea: bot"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-... | orderline: 0 | state: pick_transaction | sub_state: in_progress"

# transaction_status rows (both containers)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'status' AS container_status
   FROM transaction_status WHERE transaction_id IN ('3000000003','3000000004');"
# Expected: 2 rows, both status=SUCCESS, qty_picked=1, container_status=complete

# ItemPickedEvents on Kafka (pre-drop, danglingArea=bot)
docker exec pick-morph-camunda-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events --from-beginning --timeout-ms 3000 2>/dev/null
# Expected: two messages with danglingArea=bot, picked_qty=1 each
#   tx 3000000003: transaction_id: "d86a1c62-..._24234242", tote_id: "ToteCant4.0.A.01"
#   tx 3000000004: transaction_id: "d86a1c62-..._24234243", tote_id: "ToteCant4.0.A.01"
```

> **`gor.item_picked.events` IS published** — two events, both `status=complete` → `danglingArea="bot"`, `qty_picked=1`, `tote_id="ToteCant4.0.A.01"` each.

---

## Step 3b-1 — First Bot Pick (pick_transaction: tx 3000000003)

**What happens:** AE publishes `pick_transaction` for the first pick. Only transaction `3000000003` (`status="complete"`, `qty_picked=1`) is present in `transactions[]`.
**One `ItemPickedEvent`** is enqueued with `danglingArea="bot"`.

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:36:00.000Z","message_id":"msg_real_3b1","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[{"id":11648,"orderId":934622318,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSING","state":"pick_transaction","attributes":{"event_type":"pick_transaction","sub_state":"in_progress","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"pick_transaction"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62|Updated ae_order|Enqueued ItemPickedEvent|Enqueued OrderUpdateEvent"
# Expected:
#   "Updated ae_order for pickInstructionId: d86a1c62-..., state: pick_transaction"
#   "Enqueued ItemPickedEvent | pickInstructionId: d86a1c62-... | txId: 3000000003 | containerStatus: complete | danglingArea: bot"
#   "Enqueued OrderUpdateEvent | pickInstructionId: d86a1c62-... | orderline: 0 | state: pick_transaction | sub_state: in_progress"

# transaction_status row (tx 3000000003)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'status' AS container_status
   FROM transaction_status WHERE transaction_id = '3000000003';"
# Expected: 1 row, status=SUCCESS, qty_picked=1, container_status=complete

# ItemPickedEvent on Kafka
docker exec pick-morph-camunda-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events --from-beginning --timeout-ms 3000 2>/dev/null
# Expected: 1 event — transaction_id: "d86a1c62-..._24234242", picked_qty: 1, danglingArea: "bot", tote_id: "ToteCant4.0.A.01"
```

> **`gor.item_picked.events` IS published** — 1 event, `status=complete` → `danglingArea="bot"`, `picked_qty=1`, `tote_id="ToteCant4.0.A.01"`.

---

## Step 3b-2 — Second Bot Pick (pick_transaction: tx 3000000004)

**What happens:** AE publishes `pick_transaction` for the second pick. `actuals.containers[]` is cumulative (both 3000000003 and 3000000004), but `transactions[]` contains only the new pick `3000000004`.
`processedTxIds` already contains `3000000003` → it is skipped. **One new `ItemPickedEvent`** is enqueued for `3000000004` with `danglingArea="bot"`.

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:36:10.000Z","message_id":"msg_real_3b2","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null},{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:10.000Z","transactionId":"3000000004","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:10.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:10.000Z","transactionId":"3000000004","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:10.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[{"id":11648,"orderId":934622318,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSING","state":"pick_transaction","attributes":{"event_type":"pick_transaction","sub_state":"in_progress","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"pick_transaction"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62|Enqueued ItemPickedEvent"
# Expected:
#   "Enqueued ItemPickedEvent | pickInstructionId: d86a1c62-... | txId: 3000000004 | containerStatus: complete | danglingArea: bot"
#   NOTE: tx 3000000003 is skipped (already in processedTxIds from Step 3b-1)

# transaction_status row (tx 3000000004)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'status' AS container_status
   FROM transaction_status WHERE transaction_id IN ('3000000003','3000000004');"
# Expected: 2 rows (3000000003 from Step 3b-1, 3000000004 new), both status=SUCCESS, qty_picked=1

# ItemPickedEvents on Kafka (both picks now present)
docker exec pick-morph-camunda-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events --from-beginning --timeout-ms 3000 2>/dev/null
# Expected: 2 total events
#   tx 3000000003: transaction_id: "d86a1c62-..._24234242", picked_qty: 1, danglingArea: "bot", tote_id: "ToteCant4.0.A.01"
#   tx 3000000004: transaction_id: "d86a1c62-..._24234243", picked_qty: 1, danglingArea: "bot", tote_id: "ToteCant4.0.A.01"
```

> **`gor.item_picked.events` IS published** — 1 new event for tx `3000000004`, `danglingArea="bot"`, `picked_qty=1`, `tote_id="ToteCant4.0.A.01"`. Tx `3000000003` is deduplicated (skipped).

---

## Step 3c — Bot Picks Item with Missing Exception (pick_transaction event)

**What happens:** AE publishes `pick_transaction`. Transaction `3000000003` is a good pick (`qty_picked=1`). Transaction `3000000004` has a missing exception — nothing physically picked (`qty_picked=0`), so it appears **only in `exceptions[]`**, not in `transactions[]`.
Two `ItemPickedEvent`s are emitted: one normal (Case A), one exception-only (Case C).

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:36:30.000Z","message_id":"msg_real_3c","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","transactionId":"3000000003","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[{"id":22063,"state":"item_missing","type":null,"barcode":null,"transactionId":"3000000004","createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","containers":[],"actions":[],"products":[{"id":2389909,"uid":null,"possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:36:30.000Z","productQuantity":1,"productAttributes":null}],"containerAttributes":{"internal_order_id":24234243,"tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","trueCopyIdOf":null,"user_name":"grey7","rollcage_id":null,"pps_id":"13","destination_location":null,"location":"ToteCant4.0.A.01","bot_id":"13"},"carrier_type":null,"carrier_sub_type":null}],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[{"id":11648,"orderId":934622318,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSING","state":"pick_transaction","attributes":{"event_type":"pick_transaction","sub_state":"in_progress","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"pick_transaction"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "d86a1c62|Enqueued ItemPickedEvent"
# Expected:
#   "Enqueued ItemPickedEvent | pickInstructionId: d86a1c62-... | txId: 3000000003 | containerStatus: complete | danglingArea: bot"
#   "Enqueued ItemPickedEvent (exception-only) | pickInstructionId: d86a1c62-... | txId: d86a1c62-..._24234242 | exTxId: 3000000004 | exState: item_missing"

# transaction_status row (only tx 3000000003 — tx 3000000004 has no transactions entry)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'status' AS container_status
   FROM transaction_status WHERE transaction_id IN ('3000000003','3000000004');"
# Expected: 1 row for 3000000003 (status=SUCCESS, qty_picked=1); no row for 3000000004

# ItemPickedEvents on Kafka
docker exec pick-morph-camunda-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events --from-beginning --timeout-ms 3000 2>/dev/null
# Expected:
#   Event for tx 3000000003 (Case A): "transaction_id":"<PI_ID>_24234242", "picked_qty":1, "tote_id":"ToteCant4.0.A.01" — no "exception" field
#   Event for tx 3000000004 (Case C): "transaction_id":"<PI_ID>_24234242", "picked_qty":0, "exception":{"missing":1,"unscannable":0,"physically_damaged":0,"checklist_exception":0} — no "tote_id" (exception-only path, no transaction object)
```

> **`gor.item_picked.events` emits two events:** tx `3000000003` → good pick, `tote_id="ToteCant4.0.A.01"`, no exception; tx `3000000004` → exception-only, `picked_qty=0`, `exception.missing=1`, no `tote_id`.

---

## Step 4 — Order Complete (final update event)

**What happens:** AE publishes `update` with `state=complete`. Workflow waits for unload (Step 4c).

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:37:00.000Z","message_id":"msg_real_4","entity_id":"4feb_221","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"4feb_221","serviceRequests":[{"id":0,"externalServiceRequestId":"4feb_221a","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"3000000001","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","bot_id":"13","pps_id":"13","exceptions":[]}},{"transactionId":"3000000002","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"complete","bot_id":"13","pps_id":"13","exceptions":[]}}]},"transactions":[],"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"isDeleted":false,"stages":[{"id":11648,"orderId":2000,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"event_type":"update","sub_state":"complete","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "4feb_221|Updated ae_order|Enqueued OrderUpdateEvent"
# Expected:
#   "Updated ae_order for pickInstructionId: 4feb_221, state: complete"
#   "Enqueued OrderUpdateEvent | pickInstructionId: 4feb_221 | orderline: 0 | state: complete | sub_state: complete"
#   NOTE: transactions=[] — no transaction processing; no ItemPickedEvent

# ae_order state
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->>'state' AS sr_state,
          payload::jsonb->'serviceRequests'->0->>'status' AS sr_status,
          updated_at
   FROM ae_order WHERE external_service_request_id='4feb_221';"
# Expected: state=complete, sub_state=complete, sr_state=complete, sr_status=PROCESSED

# Camunda still running (waiting for unload)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, end_time_
   FROM act_hi_procinst WHERE business_key_='4feb_221';"
# Expected: end_time_=null (workflow still waiting for unload event in Step 4c)
```

---

## Step 4c — Pallet Unloaded (unloading event → `released`)

**What happens:** Both containers unloaded — `3000000001` and `3000000002`, each `status="unloaded"`, `qty=1`.
Both txIds seen as new unload events → **two `ItemPickedEvent`s enqueued, each with `danglingArea="undefined"` and `qty_picked=1`** (drop path).
`AEOrderTransformer` sees all containers unloaded with total `qty_picked=2 == expectation` → `ol_status=released` → workflow completes.

**Topic:** `gor.pick-list.events`

```bash
docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events <<'KAFKA_EOF'
{"source_service":"ae-order-service","timestamp":"2026-04-01T14:38:00.000Z","message_id":"msg_real_4c","entity_id":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","name":"order_information","payload":{"id":934622318,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2","serviceRequests":[{"id":0,"externalServiceRequestId":"d86a1c62-ac3e-4c1b-880b-439cfbebaac2_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","transactionId":"3000000005","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"unloaded","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null},{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","transactionId":"3000000006","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"unloaded","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","transactionId":"3000000005","products":[{"id":2389907,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":1,"status":"unloaded","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null},{"id":2389530,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","transactionId":"3000000006","products":[{"id":2389908,"uid":"2","possibleUids":null,"uidType":null,"createdOn":"2026-04-01T14:35:00.000Z","updatedOn":"2026-04-01T14:38:00.000Z","productQuantity":1,"productAttributes":{"tote_id":"ToteCant4.0.A.01","package_count":1,"pdfa_values":{"product_sku":"auto_rtp_1747908817"},"package_name":"Item","serialized_content":[],"tote_ids":["ToteCant4.0.A.01"]}}],"containerAttributes":{"internal_order_id":24234243,"lpn_id":24234243,"qty_to_be_picked":1,"qty_picked":1,"status":"unloaded","tote_id":"ToteCant4.0.A.01","pps_bin_id":"2","pps_seat_name":"ToteCant4.0.A.01","user_name":"grey7","pps_id":"13","bot_id":"13","location":"ToteCant4.0.A.01","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"productQuantity":2,"productAttributes":{"filter_parameters":["product_sku = 'auto_rtp_1747908817'"],"package_count":2,"product_sku":"auto_rtp_1747908817","package_parameters":["package_name = 'Item'"],"fragile":false}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[934622318],"has_parent":true,"location":{"displayName":"ToteCant4.0.A.01","fullAddress":"ToteCant4.0.A.01","addressFields":{"slot_id":"ToteCant4.0.A.01"}},"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[{"id":11648,"orderId":2000,"externalServiceRequestId":"0","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[24234242],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-04-01T14:17:06.000Z","status":"PROCESSED","state":"complete","attributes":{"event_type":"update","sub_state":"complete","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","palletization":true,"simple_priority":"normal"},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal"},"createdOn":"2026-04-01T14:17:06.000Z","updatedOn":"2026-04-01T14:17:06.000Z","isDeleted":false,"stages":[],"onHold":false},"context":{"execution_id":"0","event_type":"update"}}
KAFKA_EOF
```

**Verify:**

```bash
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "4feb_221|released|Enqueued ItemPickedEvent|OnWorkflowComplete|Workflow"
# Expected:
#   "Enqueued ItemPickedEvent | pickInstructionId: 4feb_221 | txId: 3000000001 | containerStatus: unloaded | danglingArea: undefined"
#   "Enqueued ItemPickedEvent | pickInstructionId: 4feb_221 | txId: 3000000002 | containerStatus: unloaded | danglingArea: undefined"
#   "Order released for pickInstructionId: 4feb_221 — triggering workflow completion"
#   "Finalizing Workflow: Pick Instruction 4feb_221 is COMPLETE."
#   "Workflow cleanup complete for pickInstructionId: 4feb_221 — final status: COMPLETED"

# 1. ae_order — status=released
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS ae_state, status,
          payload::jsonb->'serviceRequests'->0->>'status' AS ol_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at
   FROM ae_order WHERE external_service_request_id='4feb_221';"
# Expected: ae_state=complete, status=released, ol_status=released, container_status=unloaded, qty_picked=1

# 2. Camunda process instance ended
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst WHERE business_key_='4feb_221';"
# Expected: end_time_ IS NOT null (process completed)

# 3. ItemPickedEvent on Kafka
docker exec pick-morph-camunda-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events --from-beginning --timeout-ms 3000 2>/dev/null
# Expected: four messages total — two from step 3b (danglingArea=bot, txId=3000000001 and 3000000002, tote_id present); two from step 4c (danglingArea=undefined, txId=3000000001 and 3000000002, tote_id present)

# 4. All outbox events published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status, count(*) FROM outbox_event GROUP BY topic, status ORDER BY topic;"
# Expected: all rows have status=PUBLISHED
```

---

## AE Order Update Rules (from LLD)

When any `gor.pick-list.events` message is received, **only** these fields are updated in `ae_order`:

| Field | Level |
|-------|-------|
| `actuals` | root + serviceRequests |
| `expectations` | root + serviceRequests |
| `exceptions` | root + serviceRequests |
| `attributes` | root + serviceRequests |
| `state` | root + serviceRequests |
| `status` | root + serviceRequests |
| `sub_state` (inside `attributes`) | root + serviceRequests |

> `transactions` field is **ignored** — never written to `ae_order`.
> All other fields (e.g. `type`, `createdOn`, `stages`) are **never overwritten**.

### State Machine Reference

| Scenario | Order State | OL State | Order sub_state | OL sub_state |
|----------|------------|----------|-----------------|--------------|
| Order created | `created` | `created` | `created` | `created` |
| Before palletization | `cancellation_locked` | `created` | `created` | `created` |
| Sent to palletization | `cancellation_locked` | `created` | `in_palletization` | `in_palletization` |
| All pallets created | `cancellation_locked` | `created` | `fully_palletized` | `fully_palletized` |
| Internal orders created in SRMS | `cancellation_locked` | `created` | `internal_order_created` | `internal_order_created` |
| Sent for picking (mission) | `fullfillable` | `fullfillable` | `in_progress` | `in_progress` |
| Partial pallet picked | `pick_transaction` | `fullfillable` | `in_progress` | `in_progress` |
| All containers of OL picked | `pick_transaction` | `complete` | `in_progress` | `complete` |
| All OLs of order complete | `complete` | `complete` | `complete` | `complete` |
| All containers unloaded at dock | `complete` | `complete` | `complete` | `complete` |

> **`ae_order.status` (derived by AEOrderTransformer):** `created` → `pending` → `complete` → `released`
> `released` is set when all containers across all OLs have `status=unloaded` **and** `qty_picked == OL expectation`.
> When `ae_order.status = released`, the Camunda workflow completes automatically.

---

## order_update.events Outbound Payload Reference

Published to `gor.order_update.events` for every `pick-list.events` received. The `name` field (envelope level) carries the intent; `payload` carries the order data.

### Normal message (`name="update"`) — sent for every sub_state

```json
{
  "source_service": "ae-order-service",
  "name": "update",
  "entity_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
  "message_id": "<uuid>",
  "timestamp": "<iso-timestamp>",
  "context": { "execution_id": "0" },
  "payload": {
    "pick_instruction_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
    "order_id": "2000",
    "orderlineId": "0",
    "state": "cancellation_locked",
    "subState": "fully_palletized",
    "transactions": [
      {
        "transaction_id": "3000000002",
        "internal_order_id": 24234242,
        "qty_to_be_picked": 2,
        "qty_picked": 0,
        "status": "created",
        "sub_state": "fully_palletized",
        "pps_id": 13,
        "tote_id": "ToteCant4.0.A.01"
      }
    ]
  }
}
```

### Delete message (`name="delete"`) — sent only when `sub_state=fully_palletized`

A second message is published immediately after the update message. `transactions` contains only `transaction_id` (the pickInstructionId), signalling butler_core to remove the dangling entry.

```json
{
  "source_service": "ae-order-service",
  "name": "delete",
  "entity_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
  "message_id": "<uuid>",
  "timestamp": "<iso-timestamp>",
  "context": { "execution_id": "0" },
  "payload": {
    "pick_instruction_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2",
    "order_id": "2000",
    "orderlineId": "0",
    "state": "cancellation_locked",
    "subState": "fully_palletized",
    "transactions": [
      { "transaction_id": "d86a1c62-ac3e-4c1b-880b-439cfbebaac2" }
    ]
  }
}
```

### Message counts per sub_state

| sub_state | Messages on `order_update.events` | name |
|-----------|----------------------------------|------|
| `in_palletization` | 1 (remainder only, no container) | `update` |
| `partially_palletized` | 1 (container + remainder) | `update` |
| `fully_palletized` | **2** — ① container transaction, ② delete | `update`, `delete` |
| all others | 1 | `update` |

---

## Useful Queries

```bash
# Full ae_order payload
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT external_service_request_id, updated_at,
          payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state
   FROM ae_order;"

# transaction_status
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT transaction_id, pick_instruction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'bot_id' AS bot_id
   FROM transaction_status;"

# All outbox events
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT id, aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at;"

# Camunda process instances
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 5;"

# App health
curl -s http://localhost:9191/actuator/health | jq .
```

---

## Useful UIs

| Tool | URL |
|------|-----|
| Kafka UI | http://localhost:8080 |
| Camunda Cockpit | http://localhost:9191/camunda/app/cockpit (admin / admin) |
| App health | http://localhost:9191/actuator/health |

---

## Kafka Topics Reference

| Topic | Direction | Triggered in |
|-------|-----------|--------------|
| `gor.pick-instruction.requests` | Inbound (butler_core → morph) | Step 1 — trigger workflow via Kafka |
| `gor.pick-list.requests` | Outbound (morph → AE) | Step 1 — workflow start |
| `gor.pick-list.response` | Inbound (AE → morph) | Step 2 — AE accepts/rejects |
| `gor.pick-list.events` | Inbound (AE → morph) | Steps 3a, 3b, 4, 4c — all pick-list events |
| `gor.pick-instruction.response` | Outbound (morph → butler_core) | Step 2b — validation outcome |
| `gor.item_picked.events` | Outbound (morph → butler_core) | Step 3b (`status=complete`, `danglingArea=bot`), Step 4c (`status=unloaded`, `danglingArea=undefined`) |
| `gor.order_update.events` | Outbound (morph → butler_core) | Every pick-list event — `name="update"` for all sub_states; extra `name="delete"` when `sub_state=fully_palletized` |
