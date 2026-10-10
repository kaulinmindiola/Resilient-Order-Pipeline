package io.github.kaulinmindiola.rop.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.domain.model.RejectionReason;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationItem;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

/** The claim, the reservation and the outcome event commit together (ADR-0011, REQ-FUNC-020). */
class ReserveInventoryIT extends AbstractIntegrationTest {

    @Autowired private ReserveInventoryService service;
    @Autowired private JdbcClient jdbc;

    private final String prefix = "TEST-" + UUID.randomUUID();

    private String product(String suffix, int quantity) {
        String productId = prefix + suffix;
        jdbc.sql("INSERT INTO stock (product_id, quantity) VALUES (?, ?)")
                .params(productId, quantity)
                .update();
        return productId;
    }

    private int stockOf(String productId) {
        return jdbc.sql("SELECT quantity FROM stock WHERE product_id = ?")
                .param(productId)
                .query(Integer.class)
                .single();
    }

    private int claimsOf(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
    }

    private List<String> outboxOf(UUID orderId) {
        return jdbc.sql(
                        "SELECT event_type || ':' || COALESCE(payload->>'reason', '-')"
                                + " FROM outbox_events WHERE aggregate_id = ?")
                .param(orderId.toString())
                .query(String.class)
                .list();
    }

    @Test
    @DisplayName(
            "ADR-0011 REQ-FUNC-012 a reservation commits the claim, the decrement and InventoryReserved together")
    void reservationCommitsAllThreeEffects() {
        String a = product("-a", 5);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        service.handle(
                eventId, new ReservationRequest(orderId, List.of(new ReservationItem(a, 2))));

        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(stockOf(a)).isEqualTo(3);
        assertThat(outboxOf(orderId)).containsExactly("InventoryReserved:-");
    }

    @Test
    @DisplayName(
            "ADR-0010 ADR-0011 a partial rejection commits the claim and InventoryRejected, with no decrement")
    void rejectionCommitsClaimAndEventWithoutDecrement() {
        String a = product("-a", 5);
        String b = product("-b", 0);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        assertThat(
                        service.handle(
                                eventId,
                                new ReservationRequest(
                                        orderId,
                                        List.of(
                                                new ReservationItem(a, 2),
                                                new ReservationItem(b, 1)))))
                .contains(new ReservationResult.Rejected(RejectionReason.INSUFFICIENT_STOCK));

        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(stockOf(a)).isEqualTo(5);
        assertThat(outboxOf(orderId)).containsExactly("InventoryRejected:INSUFFICIENT_STOCK");
    }

    @Test
    @DisplayName("REQ-FUNC-016 the same event handled twice decrements once and writes one outcome")
    void duplicateIsAppliedOnce() {
        String a = product("-a", 5);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        ReservationRequest request =
                new ReservationRequest(orderId, List.of(new ReservationItem(a, 2)));

        service.handle(eventId, request);
        assertThat(service.handle(eventId, request)).isEmpty();

        assertThat(stockOf(a)).isEqualTo(3);
        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(outboxOf(orderId)).hasSize(1);
    }
}
