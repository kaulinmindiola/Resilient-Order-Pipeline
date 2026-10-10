package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-01-15T10:00:10Z");

    private final InMemoryOutbox outbox = new InMemoryOutbox();
    private final KafkaTemplate<String, String> kafkaTemplate = mock();
    private final OutboxPublisher publisher =
            new OutboxPublisher(
                    outbox,
                    kafkaTemplate,
                    new EventEnvelopeMapper(JsonMapper.builder().build()),
                    Clock.fixed(NOW, ZoneOffset.UTC));

    private static OutboxEvent event(String aggregateId, Instant occurredAt) {
        return new OutboxEvent(
                UUID.randomUUID(),
                "InventoryReserved",
                aggregateId,
                "inventory.reserved",
                Map.of("orderId", aggregateId),
                occurredAt);
    }

    private static CompletableFuture<SendResult<String, String>> acked() {
        return CompletableFuture.completedFuture(null);
    }

    private static CompletableFuture<SendResult<String, String>> brokerDown() {
        return CompletableFuture.failedFuture(new KafkaException("broker unavailable"));
    }

    @Test
    @DisplayName(
            "REQ-FUNC-010 ADR-0003 pending events are sent oldest first, keyed by aggregate, and marked after the ack")
    void publishesInOrderAndMarksAfterAck() {
        OutboxEvent second = event("order-b", Instant.parse("2026-01-15T10:00:02Z"));
        OutboxEvent first = event("order-a", Instant.parse("2026-01-15T10:00:01Z"));
        outbox.save(second);
        outbox.save(first);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(acked());

        int published = publisher.publishPending();

        assertThat(published).isEqualTo(2);
        InOrder sends = inOrder(kafkaTemplate);
        sends.verify(kafkaTemplate).send(eq("inventory.reserved"), eq("order-a"), anyString());
        sends.verify(kafkaTemplate).send(eq("inventory.reserved"), eq("order-b"), anyString());
        assertThat(outbox.publishedAt(first.eventId())).isEqualTo(NOW);
        assertThat(outbox.publishedAt(second.eventId())).isEqualTo(NOW);
    }

    @Test
    @DisplayName("REQ-FUNC-011 ADR-0001 the first failure ends the batch and nothing is marked")
    void firstFailureEndsTheBatch() {
        OutboxEvent first = event("order-a", Instant.parse("2026-01-15T10:00:01Z"));
        OutboxEvent second = event("order-b", Instant.parse("2026-01-15T10:00:02Z"));
        outbox.save(first);
        outbox.save(second);
        when(kafkaTemplate.send(anyString(), eq("order-a"), anyString())).thenReturn(brokerDown());

        int published = publisher.publishPending();

        assertThat(published).isZero();
        verify(kafkaTemplate, never()).send(anyString(), eq("order-b"), anyString());
        assertThat(outbox.findPending(10)).containsExactly(first, second);
    }

    @Test
    @DisplayName("REQ-FUNC-011 a failed event is published on a later cycle without intervention")
    void failedEventIsRetriedOnNextCycle() {
        OutboxEvent event = event("order-a", Instant.parse("2026-01-15T10:00:01Z"));
        outbox.save(event);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(brokerDown())
                .thenReturn(acked());

        assertThat(publisher.publishPending()).isZero();
        assertThat(publisher.publishPending()).isEqualTo(1);

        assertThat(outbox.findPending(10)).isEmpty();
    }

    @Test
    @DisplayName(
            "ADR-0001 ADR-0011 if marking fails after the ack, the event is sent again (expected duplicate)")
    void markFailureLeadsToRepublication() {
        OutboxEvent event = event("order-a", Instant.parse("2026-01-15T10:00:01Z"));
        outbox.save(event);
        outbox.failNextMark();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(acked());

        assertThat(publisher.publishPending()).isZero();
        assertThat(publisher.publishPending()).isEqualTo(1);

        verify(kafkaTemplate, times(2)).send(eq("inventory.reserved"), eq("order-a"), anyString());
        assertThat(outbox.publishedAt(event.eventId())).isEqualTo(NOW);
    }
}
