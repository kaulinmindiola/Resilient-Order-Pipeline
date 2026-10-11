package io.github.kaulinmindiola.rop.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import io.github.kaulinmindiola.rop.order.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.order.domain.port.OrderRepository;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * ADR-0011: if saving the transition fails, the claim is rolled back too, so the redelivered event
 * is applied instead of being mistaken for a duplicate.
 */
class ApplyInventoryOutcomeAtomicityIT extends AbstractIntegrationTest {

    @Autowired private ApplyInventoryOutcomeService service;
    @Autowired private CreateOrderService createOrderService;
    @Autowired private JdbcClient jdbc;
    @MockitoSpyBean private OrderRepository orderRepository;

    @Test
    @DisplayName(
            "ADR-0011 a failed transition rolls back its claim; the redelivery confirms the order")
    void failureRollsBackClaimAndRetryApplies() {
        UUID orderId =
                createOrderService
                        .create(
                                new CreateOrderCommand(
                                        List.of(new CreateOrderCommand.Item("SKU-001", 1))))
                        .id();
        UUID eventId = UUID.randomUUID();
        doThrow(new IllegalStateException("simulated failure while saving the order"))
                .doCallRealMethod()
                .when(orderRepository)
                .save(any());

        assertThatThrownBy(() -> service.apply(eventId, orderId, InventoryOutcome.RESERVED))
                .hasMessage("simulated failure while saving the order");
        int claimsAfterFailure =
                jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                        .param(eventId)
                        .query(Integer.class)
                        .single();
        assertThat(claimsAfterFailure).isZero();

        assertThat(service.apply(eventId, orderId, InventoryOutcome.RESERVED))
                .isEqualTo(OutcomeHandling.TRANSITION_APPLIED);
        String status =
                jdbc.sql("SELECT status FROM orders WHERE id = ?")
                        .param(orderId)
                        .query(String.class)
                        .single();
        assertThat(status).isEqualTo("CONFIRMED");
    }
}
