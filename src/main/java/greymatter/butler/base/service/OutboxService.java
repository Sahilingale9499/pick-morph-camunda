package greymatter.butler.base.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import greymatter.butler.base.dto.KafkaEventEnvelope;
import greymatter.butler.base.event.OutboxMessageEvent;
import greymatter.butler.base.model.Outbox;
import greymatter.butler.base.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Transactional outbox service for reliable Kafka publishing.
 *
 * Writes outbox rows atomically with domain DB changes (same transaction).
 * After commit, immediately attempts to publish to Kafka via @TransactionalEventListener.
 * A background poller retries any failed publishes as a fallback.
 */
@Service
@Slf4j
public class OutboxService {

    private static final String SOURCE_SERVICE = "ae-order-service";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final JdbcTemplate jdbcTemplate;

    @Value("${outbox.publish.max-retries:3}")
    private int maxRetries;

    @Value("${outbox.publish.retry-delay-ms:200}")
    private long retryDelayMs;

    public OutboxService(OutboxEventRepository outboxEventRepository,
                         KafkaTemplate<String, Object> kafkaTemplate,
                         ObjectMapper objectMapper,
                         ApplicationEventPublisher applicationEventPublisher,
                         JdbcTemplate jdbcTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.applicationEventPublisher = applicationEventPublisher;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Write an outbox entry and schedule immediate after-commit publishing.
     * Wraps {@code payload} in a {@link KafkaEventEnvelope} before serializing.
     * MUST be called within an existing @Transactional boundary (Propagation.MANDATORY).
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void save(String topic, String key, Object payload, String eventName) {
        try {
            KafkaEventEnvelope<Object> envelope = new KafkaEventEnvelope<>();
            envelope.setSourceService(SOURCE_SERVICE);
            envelope.setTimestamp(Instant.now().toString());
            envelope.setMessageId(UUID.randomUUID().toString());
            envelope.setEntityId(key);
            envelope.setName(eventName);
            envelope.setPayload(payload);
            KafkaEventEnvelope.Context ctx = new KafkaEventEnvelope.Context();
            ctx.setExecutionId("0");
            envelope.setContext(ctx);

            String payloadJson = objectMapper.writeValueAsString(envelope);
            UUID id = UUID.randomUUID();
            jdbcTemplate.update(
                "INSERT INTO outbox (id, topic, message_key, payload, published, created_at) " +
                "VALUES (?::uuid, ?, ?, ?::jsonb, false, now())",
                id.toString(), topic, key, payloadJson
            );
            applicationEventPublisher.publishEvent(new OutboxMessageEvent(this, id));
        } catch (Exception e) {
            throw new RuntimeException("Failed to write outbox event for topic: " + topic, e);
        }
    }

    /**
     * Fires immediately after the transaction commits. Attempts to publish the
     * outbox entry to Kafka with configurable retries.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void onOutbox(OutboxMessageEvent event) {
        Outbox outbox = outboxEventRepository.findUnpublishedByIdForUpdate(event.getOutboxEventId()).orElse(null);
        if (outbox == null) {
            return;
        }

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Object parsedPayload = parsePayload(outbox.getPayload());
                kafkaTemplate.send(outbox.getTopic(), outbox.getMessageKey(), parsedPayload).get();
                jdbcTemplate.update(
                    "UPDATE outbox SET published = true, published_at = now() WHERE id = ?::uuid",
                    outbox.getId().toString()
                );
                return;
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("Outbox publish attempt {}/{} failed for id={} topic={} — retrying in {}ms",
                            attempt, maxRetries, outbox.getId(), outbox.getTopic(), retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.warn("All {} immediate retries failed for outbox id={} topic={} — poller will handle",
                            maxRetries, outbox.getId(), outbox.getTopic());
                }
            }
        }
    }

    /**
     * Background poller — fallback for entries that failed immediate publishing.
     */
    @Scheduled(fixedDelayString = "${outbox.poller.interval-ms:5000}")
    @Transactional
    public void publishPendingEvents() {
        List<Outbox> pending = outboxEventRepository.findPendingForUpdate();
        if (pending.isEmpty()) {
            return;
        }

        log.info("Outbox poller: found {} unpublished events", pending.size());
        for (Outbox event : pending) {
            try {
                Object parsedPayload = parsePayload(event.getPayload());
                kafkaTemplate.send(event.getTopic(), event.getMessageKey(), parsedPayload).get();
                jdbcTemplate.update(
                    "UPDATE outbox SET published = true, published_at = now() WHERE id = ?::uuid",
                    event.getId().toString()
                );
                log.info("Outbox poller: published id={} topic={}", event.getId(), event.getTopic());
            } catch (Exception e) {
                log.warn("Outbox poller: failed to publish id={} topic={} — will retry next cycle",
                        event.getId(), event.getTopic());
                break;
            }
        }
    }

    private Object parsePayload(String payloadJson) {
        try {
            return objectMapper.readTree(payloadJson);
        } catch (Exception e) {
            log.warn("Failed to parse outbox payload as JSON — sending as raw string");
            return payloadJson;
        }
    }
}
