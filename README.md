# Pick Instruction Workflow

A Spring Boot + Camunda 7 + Kafka application for orchestrating pick instruction workflows.

## Tech stack
- Java 17 / Spring Boot 3.5
- Camunda BPM 7.22 (embedded process engine)
- Kafka (KRaft mode via Docker)
- PostgreSQL (app data + Camunda schema)
- gRPC (Butler Core integration)

## Architecture

```
REST API  →  PickInstructionProcessService  →  Camunda RuntimeService
                                                      │
                                          pickInstructionProcess (BPMN)
                                                      │
                        ┌─────────────────────────────┼──────────────────────────┐
                        │                             │                          │
               JavaDelegate beans              Message Catch Events        Kafka producers
         (PublishOrderToKafka,           (PickListResponseMessage,       (orders-topic,
          UpdatePickInstruction,          TransactionUpdateMessage)       transaction-events,
          MarkComplete/Failed, …)                     │                   workflow-complete)
                                                      │
                                           Kafka Listeners correlate
                                           messages back to process
```

**Workflow steps** (modelled in `pick-instruction-workflow.bpmn`):
1. Publish order to Kafka
2. Wait for `PickListResponseMessage` (from `validation-results-topic`)
3. Check if pick instruction already complete
4. Loop: wait for `TransactionUpdateMessage` (from `transaction-updates-topic`)
5. Route on command — `UPDATE` / `CANCEL` / `COMPLETE` / `RETRY`
6. Call Butler Core gRPC for updates; handle retriable vs non-retriable errors
7. On any terminal path, run `onWorkflowComplete` for audit + cleanup

## Running locally

### Prerequisites
- Java 17+
- PostgreSQL running on `localhost:5432` with a database named `app_db`
- Docker (for Kafka)

### 1. Start Kafka
```bash
docker compose up -d kafka
```

### 2. Run the app
```bash
mvn spring-boot:run
```

App starts on **http://localhost:9191**.

> Butler Core gRPC defaults to **mock mode** locally (`butler.core.grpc.mock-enabled=true`).
> Set `BUTLER_CORE_MOCK_ENABLED=false` and `BUTLER_GRPC_ADDR=<host>:<port>` when connecting to a real Butler Core.

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/Order/pick_instruction` | Start a pick instruction workflow |
| `POST` | `/Order/validate` | Publish validation result to Kafka |
| `POST` | `/Order/transaction-update` | Publish transaction update to Kafka |
| `DELETE` | `/Order/terminate/{pickId}` | Terminate a running process instance |
| `GET` | `/actuator/health` | Health check (includes Camunda process engine) |
| `GET` | `/ratelimit/metrics` | Request metrics snapshot |

### Example flow

```bash
# 1. Start workflow
curl -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{"pickId":"Pick-001","item":"Screwdriver","tpid":"TPID-100","qty":2,"uom":"BOX",
       "pickLocation":"ZONE-A","dropLocation":"PACK-01","ppsId":1,"binId":"BIN-01",
       "ppsPoint":"PPS-01","seatName":"SEAT-01","slotref":"SLOT-01","userLoggedIn":"op1"}'

# 2. Send validation result
curl -X POST http://localhost:9191/Order/validate \
  -H "Content-Type: application/json" \
  -d '{"orderId":"Pick-001","transactionId":"TXN-001","success":true}'

# 3. Complete the workflow
curl -X POST http://localhost:9191/Order/transaction-update \
  -H "Content-Type: application/json" \
  -d '{"pickId":"Pick-001","transactionId":"TXN-002","command":"COMPLETE","status":"COMPLETED","transactionType":"PICK","processedQty":2}'
```

## Kafka topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `orders-topic` | Outbound | Publishes pick orders downstream |
| `validation-results-topic` | Inbound | Validation results → correlate `PickListResponseMessage` |
| `transaction-updates-topic` | Inbound | Transaction updates → correlate `TransactionUpdateMessage` |
| `transaction-events-topic` | Outbound | Per-transaction audit events |
| `workflow-complete-events-topic` | Outbound | Final workflow audit event |

## Configuration (`application.properties`)

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `9191` | HTTP port |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/app_db` | PostgreSQL URL |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka brokers |
| `butler.core.grpc.address` | `gmc_butler_server:9090` | Butler Core gRPC address |
| `butler.core.grpc.mock-enabled` | `true` | Enable mock mode for local dev |
| `camunda.bpm.database.schema-update` | `true` | Auto-create Camunda schema |

## Project layout

```
src/main/
├── java/com/temporallearn/spring_temporal/
│   ├── controller/         # REST endpoints (PickWorkflowController)
│   ├── delegates/          # Camunda JavaDelegate service task implementations
│   ├── service/            # PickInstructionService (business logic)
│   │                       # PickInstructionProcessService (start/terminate processes)
│   ├── downstream/listener/ # Kafka listeners → Camunda message correlation
│   ├── dto/                # Request/response DTOs
│   ├── grpc/               # Butler Core gRPC client
│   ├── model/              # JPA entities (TransactionStatus)
│   ├── repository/         # Spring Data repositories
│   └── config/             # Kafka configuration
└── resources/
    ├── application.properties
    ├── pick-instruction-workflow.bpmn   # Camunda BPMN process definition
    └── db/migration/                    # Flyway migrations (disabled locally)
```

## Autoscaler

The `autoscale.sh` script monitors container memory and scales the Docker Swarm service when needed.

```bash
docker info --format '{{.Swarm.LocalNodeState}}' | grep -q active || docker swarm init
docker compose up -d
./autoscale.sh > autoscale.log 2>&1 &
tail -f autoscale.log
```
