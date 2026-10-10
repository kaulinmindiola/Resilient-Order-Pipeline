package io.github.kaulinmindiola.rop.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationItem;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * ADR-0011: if any effect fails, nothing is committed, including the claim, so the redelivered
 * event is processed from scratch and reserves stock exactly once.
 */
class ReserveInventoryAtomicityIT extends AbstractIntegrationTest {

    @Autowired private ReserveInventoryService service;
    @Autowired private JdbcClient jdbc;
    @MockitoSpyBean private OutboxEventRepository outbox;

    private int count(String sql, Object param) {
        return jdbc.sql(sql).param(param).query(Integer.class).single();
    }

    @Test
    @DisplayName(
            "ADR-0011 an outbox failure rolls back the claim and the decrement; the retry applies once")
    void failureRollsBackEverythingAndRetryAppliesOnce() {
        String productId = "TEST-" + UUID.randomUUID();
        jdbc.sql("INSERT INTO stock (product_id, quantity) VALUES (?, 5)")
                .param(productId)
                .update();
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        ReservationRequest request =
                new ReservationRequest(orderId, List.of(new ReservationItem(productId, 2)));
        doThrow(new IllegalStateException("simulated outbox failure"))
                .doCallRealMethod()
                .when(outbox)
                .save(any());

        assertThatThrownBy(() -> service.handle(eventId, request))
                .hasMessage("simulated outbox failure");
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", eventId))
                .isZero();
        assertThat(count("SELECT quantity FROM stock WHERE product_id = ?", productId))
                .isEqualTo(5);

        service.handle(eventId, request);

        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", eventId))
                .isEqualTo(1);
        assertThat(count("SELECT quantity FROM stock WHERE product_id = ?", productId))
                .isEqualTo(3);
        assertThat(
                        count(
                                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ?",
                                orderId.toString()))
                .isEqualTo(1);
    }
}
