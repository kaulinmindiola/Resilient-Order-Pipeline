package io.github.kaulinmindiola.rop.inventory.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.model.RejectionReason;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationItem;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationResult;
import io.github.kaulinmindiola.rop.inventory.domain.port.ProcessedEventRepository;
import io.github.kaulinmindiola.rop.inventory.domain.port.StockRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReserveInventoryServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-15T10:00:00Z");
    private static final UUID ORDER_ID = UUID.fromString("77777777-7777-7777-7777-777777777777");

    private final Set<String> claims = new HashSet<>();
    private final ProcessedEventRepository processedEvents =
            (eventId, consumer, at) -> claims.add(eventId + "|" + consumer);

    private final AtomicInteger reserveCalls = new AtomicInteger();
    private ReservationResult stockAnswer = new ReservationResult.Reserved();
    private final StockRepository stockRepository =
            request -> {
                reserveCalls.incrementAndGet();
                return stockAnswer;
            };

    private final RecordingOutboxEventRepository outbox = new RecordingOutboxEventRepository();
    private final ReserveInventoryService service =
            new ReserveInventoryService(
                    processedEvents, stockRepository, outbox, Clock.fixed(NOW, ZoneOffset.UTC));

    private static ReservationRequest request() {
        return new ReservationRequest(ORDER_ID, List.of(new ReservationItem("SKU-001", 2)));
    }

    @Test
    @DisplayName(
            "REQ-FUNC-012 REQ-FUNC-020 a successful reservation writes InventoryReserved to the outbox")
    void reservationWritesInventoryReserved() {
        assertThat(service.handle(UUID.randomUUID(), request()))
                .contains(new ReservationResult.Reserved());

        assertThat(outbox.saved()).hasSize(1);
        OutboxEvent event = outbox.saved().getFirst();
        assertThat(event.eventType()).isEqualTo("InventoryReserved");
        assertThat(event.topic()).isEqualTo("inventory.reserved");
        assertThat(event.aggregateId()).isEqualTo(ORDER_ID.toString());
        assertThat(event.occurredAt()).isEqualTo(NOW);
        assertThat(event.payload()).isEqualTo(Map.of("orderId", ORDER_ID.toString()));
    }

    @Test
    @DisplayName("REQ-FUNC-013 REQ-FUNC-020 a rejection writes InventoryRejected with its reason")
    void rejectionWritesInventoryRejected() {
        stockAnswer = new ReservationResult.Rejected(RejectionReason.UNKNOWN_PRODUCT);

        assertThat(service.handle(UUID.randomUUID(), request())).contains(stockAnswer);

        OutboxEvent event = outbox.saved().getFirst();
        assertThat(event.eventType()).isEqualTo("InventoryRejected");
        assertThat(event.topic()).isEqualTo("inventory.rejected");
        assertThat(event.payload())
                .isEqualTo(Map.of("orderId", ORDER_ID.toString(), "reason", "UNKNOWN_PRODUCT"));
    }

    @Test
    @DisplayName("REQ-FUNC-016 BR-007 a duplicate delivery neither reserves nor writes again")
    void duplicateIsANoOp() {
        UUID eventId = UUID.randomUUID();

        service.handle(eventId, request());
        assertThat(service.handle(eventId, request())).isEmpty();

        assertThat(reserveCalls).hasValue(1);
        assertThat(outbox.saved()).hasSize(1);
    }

    @Test
    @DisplayName("ADR-0011 the event is claimed under this service's consumer name")
    void claimsUnderConsumerName() {
        UUID eventId = UUID.randomUUID();

        service.handle(eventId, request());

        assertThat(claims).containsExactly(eventId + "|inventory-service");
    }
}
