package io.github.kaulinmindiola.rop.order.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.order.AbstractIntegrationTest;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

/** The claim and the order transition commit together against the real database (ADR-0011). */
class ApplyInventoryOutcomeIT extends AbstractIntegrationTest {

    @Autowired private ApplyInventoryOutcomeService service;
    @Autowired private CreateOrderService createOrderService;
    @Autowired private JdbcClient jdbc;

    private UUID pendingOrder() {
        return createOrderService
                .create(new CreateOrderCommand(List.of(new CreateOrderCommand.Item("SKU-001", 1))))
                .id();
    }

    private String statusOf(UUID orderId) {
        return jdbc.sql("SELECT status FROM orders WHERE id = ?")
                .param(orderId)
                .query(String.class)
                .single();
    }

    private OffsetDateTime updatedAtOf(UUID orderId) {
        return jdbc.sql("SELECT updated_at FROM orders WHERE id = ?")
                .param(orderId)
                .query(OffsetDateTime.class)
                .single();
    }

    private int claimsOf(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
    }

    @Test
    @DisplayName(
            "BR-004 ADR-0011 InventoryReserved confirms the order and records the claim in the same commit")
    void confirmationIsPersistedWithItsClaim() {
        UUID orderId = pendingOrder();
        UUID eventId = UUID.randomUUID();

        service.apply(eventId, orderId, InventoryOutcome.RESERVED);

        assertThat(statusOf(orderId)).isEqualTo("CONFIRMED");
        assertThat(claimsOf(eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName(
            "BR-014 BR-017 a contradictory outcome leaves a terminal order and its updated_at untouched")
    void terminalOrderIsUntouched() {
        UUID orderId = pendingOrder();
        service.apply(UUID.randomUUID(), orderId, InventoryOutcome.REJECTED);
        OffsetDateTime rejectedAt = updatedAtOf(orderId);
        UUID contradictoryEvent = UUID.randomUUID();

        assertThat(service.apply(contradictoryEvent, orderId, InventoryOutcome.RESERVED))
                .isEqualTo(OutcomeHandling.TRANSITION_IGNORED);

        assertThat(statusOf(orderId)).isEqualTo("REJECTED");
        assertThat(updatedAtOf(orderId)).isEqualTo(rejectedAt);
        assertThat(claimsOf(contradictoryEvent)).isEqualTo(1);
    }

    @Test
    @DisplayName("REQ-FUNC-019 an outcome for an unknown order is recorded as processed")
    void unknownOrderIsRecordedAsProcessed() {
        UUID eventId = UUID.randomUUID();

        assertThat(service.apply(eventId, UUID.randomUUID(), InventoryOutcome.RESERVED))
                .isEqualTo(OutcomeHandling.UNKNOWN_ORDER);

        assertThat(claimsOf(eventId)).isEqualTo(1);
    }
}
