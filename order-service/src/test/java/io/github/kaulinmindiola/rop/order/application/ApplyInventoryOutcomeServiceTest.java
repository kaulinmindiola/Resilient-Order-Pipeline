package io.github.kaulinmindiola.rop.order.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.order.domain.model.Money;
import io.github.kaulinmindiola.rop.order.domain.model.Order;
import io.github.kaulinmindiola.rop.order.domain.model.OrderItem;
import io.github.kaulinmindiola.rop.order.domain.model.OrderStatus;
import io.github.kaulinmindiola.rop.order.domain.port.ProcessedEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ApplyInventoryOutcomeServiceTest {

    private static final Instant CREATED = Instant.parse("2026-01-15T10:00:00Z");
    private static final Instant NOW = Instant.parse("2026-01-15T10:00:07Z");

    private final Set<String> claims = new HashSet<>();
    private final ProcessedEventRepository processedEvents =
            (eventId, consumer, at) -> claims.add(eventId + "|" + consumer);
    private final InMemoryOrderRepository orders = new InMemoryOrderRepository();
    private final ApplyInventoryOutcomeService service =
            new ApplyInventoryOutcomeService(
                    processedEvents, orders, Clock.fixed(NOW, ZoneOffset.UTC));

    private Order pendingOrder() {
        Order order =
                Order.create(
                        UUID.randomUUID(),
                        List.of(new OrderItem("SKU-001", 1, Money.of("19.99"))),
                        CREATED);
        orders.save(order);
        return order;
    }

    @Test
    @DisplayName("BR-004 REQ-FUNC-014 InventoryReserved confirms a PENDING order")
    void reservedConfirmsOrder() {
        Order order = pendingOrder();

        OutcomeHandling handling =
                service.apply(UUID.randomUUID(), order.id(), InventoryOutcome.RESERVED);

        assertThat(handling).isEqualTo(OutcomeHandling.TRANSITION_APPLIED);
        assertThat(order.status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order.updatedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("BR-005 REQ-FUNC-015 InventoryRejected rejects a PENDING order")
    void rejectedRejectsOrder() {
        Order order = pendingOrder();

        OutcomeHandling handling =
                service.apply(UUID.randomUUID(), order.id(), InventoryOutcome.REJECTED);

        assertThat(handling).isEqualTo(OutcomeHandling.TRANSITION_APPLIED);
        assertThat(order.status()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    @DisplayName(
            "BR-014 REQ-FUNC-019 InventoryReserved on a REJECTED order is an ignored no-op, recorded as processed")
    void invalidTransitionIsIgnored() {
        Order order = pendingOrder();
        service.apply(UUID.randomUUID(), order.id(), InventoryOutcome.REJECTED);
        int savesBefore = orders.saves();
        UUID contradictoryEvent = UUID.randomUUID();

        OutcomeHandling handling =
                service.apply(contradictoryEvent, order.id(), InventoryOutcome.RESERVED);

        assertThat(handling).isEqualTo(OutcomeHandling.TRANSITION_IGNORED);
        assertThat(order.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(orders.saves()).isEqualTo(savesBefore);
        assertThat(claims).contains(contradictoryEvent + "|order-service");
    }

    @Test
    @DisplayName("REQ-FUNC-019 an outcome for an unknown order is a no-op, recorded as processed")
    void unknownOrderIsIgnored() {
        UUID eventId = UUID.randomUUID();

        OutcomeHandling handling =
                service.apply(eventId, UUID.randomUUID(), InventoryOutcome.RESERVED);

        assertThat(handling).isEqualTo(OutcomeHandling.UNKNOWN_ORDER);
        assertThat(claims).contains(eventId + "|order-service");
    }

    @Test
    @DisplayName("REQ-FUNC-016 a duplicate is recognised as such, not as an invalid transition")
    void duplicateIsRecognised() {
        Order order = pendingOrder();
        UUID eventId = UUID.randomUUID();
        service.apply(eventId, order.id(), InventoryOutcome.RESERVED);

        OutcomeHandling handling = service.apply(eventId, order.id(), InventoryOutcome.RESERVED);

        assertThat(handling).isEqualTo(OutcomeHandling.DUPLICATE);
        assertThat(orders.saves()).isEqualTo(2);
    }
}
