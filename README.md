# Pick Instruction Workflow

A Spring Boot + Camunda 7 + Kafka application for orchestrating pick instruction workflows.

## Tech stack
- Java 17 / Spring Boot 3.5
- Camunda BPM 7.22 (embedded process engine)
- Kafka (KRaft mode via Docker)
- PostgreSQL (app data + Camunda schema)
- gRPC (Butler Pick Order integration)

## Architecture

```
REST API  →  PickInstructionProcessService  →  Camunda RuntimeService
                                                      │
                                          pickInstructionProcess (BPMN)
                                                      │
                        ┌─────────────────────────────┼──────────────────────────┐
                        │                             │                          │
               JavaDelegate beans              Message Catch Events        Kafka producers
         (PublishOrderToKafka,           (ValidationResultMessage,       (pick-list.requests,
          UpdatePickInstruction,          TransactionUpdateMessage)       item_picked.events,
          MarkComplete/Failed, …)                     │                   order_update.events)
                                                      │
                                           Kafka Listeners correlate
                                           messages back to process
```

**Workflow steps** (modelled in `pick-instruction-workflow.bpmn`):
1. Publish pick-list order to Kafka (`pick-list.requests`)
2. Wait for `ValidationResultMessage` (correlated from `pick-list.response`)
3. Check if pick instruction already complete
4. Loop: wait for `TransactionUpdateMessage` (correlated from `pick-list.events`)
5. Route on command — `UPDATE` / `CANCEL` / `COMPLETE` / `RETRY`
6. Call Butler Pick Order gRPC for updates; handle retriable vs non-retriable errors
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

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/Order/pick_instruction` | Start a pick instruction workflow |
| `DELETE` | `/Order/terminate/{pickId}` | Terminate a running process instance |
| `GET` | `/actuator/health` | Health check (includes Camunda process engine) |
| `GET` | `/ratelimit/metrics` | Request metrics snapshot |

### Example flow

```bash
# Start workflow
curl -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{"pickId":"Pick-001","item":"Screwdriver","tpid":"TPID-100","qty":2,"uom":"BOX",
       "pickLocation":"ZONE-A","dropLocation":"PACK-01","ppsId":1,"binId":"BIN-01",
       "ppsPoint":"PPS-01","seatName":"SEAT-01","slotref":"SLOT-01","userLoggedIn":"op1"}'
```

## Kafka topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `{prefix}.pick-instruction.requests` | Inbound | Kafka trigger to start a pick instruction workflow |
| `{prefix}.pick-list.requests` | Outbound | Publishes pick orders to AE |
| `{prefix}.pick-list.response` | Inbound | AE response → correlates `ValidationResultMessage` |
| `{prefix}.pick-list.events` | Inbound | AE pick events → correlates `TransactionUpdateMessage` |
| `{prefix}.item_picked.events` | Outbound | Per-transaction item-picked audit event |
| `{prefix}.order_update.events` | Outbound | Order state update events for Butler Core |

## Configuration (`application.properties`)

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `9191` | HTTP port |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/app_db` | PostgreSQL URL |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka brokers |
| `butler.pick.grpc.address` | `gmc_butler_server:50051` | Butler Pick Order gRPC address |
| `butler.pick.grpc.mock-enabled` | `false` | Enable mock mode for local dev |
| `camunda.bpm.database.schema-update` | `true` | Auto-create Camunda schema |

## Project layout

```
src/main/
├── java/com/butler/aeorder/
│   ├── controller/         # REST endpoints (PickWorkflowController)
│   ├── delegates/          # Camunda JavaDelegate service task implementations
│   ├── service/            # PickInstructionService (business logic)
│   │                       # PickInstructionProcessService (start/terminate processes)
│   ├── downstream/listener/ # Kafka listeners → Camunda message correlation
│   ├── dto/                # Request/response DTOs
│   ├── grpc/               # Butler Pick Order gRPC client
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
