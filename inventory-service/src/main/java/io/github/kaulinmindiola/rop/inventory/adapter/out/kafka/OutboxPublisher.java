package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import io.github.kaulinmindiola.rop.inventory.LogFields;
import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Polling publisher of the transactional outbox (ADR-0001, REQ-FUNC-010, REQ-FUNC-011).
 *
 * <p>Events are sent oldest first with key = aggregateId (ADR-0003) and marked only after the
 * broker acknowledged them. The first failure ends the batch, preserving per-aggregate order; the
 * event stays pending and is retried on the next cycle with no attempt limit. If sending succeeds
 * but marking fails, the event is sent again later: the duplicate is absorbed by idempotent
 * consumers (ADR-0011).
 *
 * <p>Deliberately not transactional: no database transaction is held open while waiting for Kafka.
 */
@Component
public class OutboxPublisher {

    static final int BATCH_SIZE = 100;
    static final Duration SEND_TIMEOUT = Duration.ofSeconds(15);

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventEnvelopeMapper envelopeMapper;
    private final Clock clock;

    public OutboxPublisher(
            OutboxEventRepository outbox,
            KafkaTemplate<String, String> kafkaTemplate,
            EventEnvelopeMapper envelopeMapper,
            Clock clock) {
        this.outbox = outbox;
        this.kafkaTemplate = kafkaTemplate;
        this.envelopeMapper = envelopeMapper;
        this.clock = clock;
    }

    /**
     * Publishes pending events until the batch is exhausted or the first failure.
     *
     * @return how many events were published and marked in this cycle
     */
    public int publishPending() {
        int published = 0;
        for (OutboxEvent event : outbox.findPending(BATCH_SIZE)) {
            if (!publish(event)) {
                break;
            }
            published++;
        }
        return published;
    }

    private boolean publish(OutboxEvent event) {
        MDC.put(LogFields.EVENT_ID, event.eventId().toString());
        MDC.put(LogFields.ORDER_ID, event.aggregateId());
        try {
            kafkaTemplate
                    .send(event.topic(), event.aggregateId(), envelopeMapper.toEnvelopeJson(event))
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            outbox.markPublished(event.eventId(), clock.instant());
            log.info("Outbox event {} published to {}", event.eventType(), event.topic());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Outbox publishing interrupted; the event stays pending");
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            log.warn(
                    "Outbox event not published or not marked; it stays pending for the next cycle: {}",
                    e.toString());
            return false;
        } finally {
            MDC.remove(LogFields.EVENT_ID);
            MDC.remove(LogFields.ORDER_ID);
        }
    }
}
