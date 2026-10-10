package io.github.kaulinmindiola.rop.inventory.adapter.out.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

class OutboxEventRepositoryIT extends AbstractIntegrationTest {

    private static final Instant T1 = Instant.parse("2026-01-15T10:00:01Z");
    private static final Instant T2 = Instant.parse("2026-01-15T10:00:02Z");
    private static final Instant T3 = Instant.parse("2026-01-15T10:00:03Z");

    @Autowired private OutboxEventRepository outbox;
    @Autowired private JdbcClient jdbc;

    private static OutboxEvent inventoryReserved(Instant occurredAt) {
        String orderId = UUID.randomUUID().toString();
        return new OutboxEvent(
                UUID.randomUUID(),
                "InventoryReserved",
                orderId,
                "inventory.reserved",
                Map.of(
                        "orderId",
                        orderId,
                        "items",
                        List.of(
                                Map.of("productId", "SKU-001", "quantity", 3),
                                Map.of("productId", "SKU-003", "quantity", 2))),
                occurredAt);
    }

    /** Other tests share the database: only look at the events created by this test. */
    private List<OutboxEvent> pendingAmong(OutboxEvent... events) {
        Set<UUID> ids =
                java.util.Arrays.stream(events)
                        .map(OutboxEvent::eventId)
                        .collect(Collectors.toSet());
        return outbox.findPending(10_000).stream()
                .filter(event -> ids.contains(event.eventId()))
                .toList();
    }

    @Test
    @DisplayName("BR-008 an outbox event survives a round trip, including its nested jsonb payload")
    void roundTrip() {
        OutboxEvent event = inventoryReserved(T1);

        outbox.save(event);

        assertThat(pendingAmong(event)).containsExactly(event);
    }

    @Test
    @DisplayName("REQ-FUNC-010 pending events are returned oldest first")
    void pendingEventsAreOrderedByOccurrence() {
        OutboxEvent third = inventoryReserved(T3);
        OutboxEvent first = inventoryReserved(T1);
        OutboxEvent second = inventoryReserved(T2);
        outbox.save(third);
        outbox.save(first);
        outbox.save(second);

        assertThat(pendingAmong(first, second, third))
                .extracting(OutboxEvent::eventId)
                .containsExactly(first.eventId(), second.eventId(), third.eventId());
    }

    @Test
    @DisplayName("REQ-FUNC-010 a published event is no longer pending")
    void publishedEventIsNoLongerPending() {
        OutboxEvent event = inventoryReserved(T1);
        outbox.save(event);

        outbox.markPublished(event.eventId(), T2);

        assertThat(pendingAmong(event)).isEmpty();
        OffsetDateTime publishedAt =
                jdbc.sql("SELECT published_at FROM outbox_events WHERE event_id = ?")
                        .param(event.eventId())
                        .query(OffsetDateTime.class)
                        .single();
        assertThat(publishedAt.toInstant()).isEqualTo(T2);
    }

    @Test
    @DisplayName("findPending never returns more events than the limit")
    void respectsLimit() {
        outbox.save(inventoryReserved(T1));
        outbox.save(inventoryReserved(T2));
        outbox.save(inventoryReserved(T3));

        assertThat(outbox.findPending(2)).hasSize(2);
    }
}
