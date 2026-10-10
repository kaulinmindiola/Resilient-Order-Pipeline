package io.github.kaulinmindiola.rop.inventory.application;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationResult;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import io.github.kaulinmindiola.rop.inventory.domain.port.ProcessedEventRepository;
import io.github.kaulinmindiola.rop.inventory.domain.port.StockRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Handles an OrderCreated event in a single local transaction (ADR-0011): claim the event, reserve
 * stock all-or-nothing, and write the outcome to this service's outbox (ADR-0013, REQ-FUNC-020).
 * The three effects commit together or not at all. It never talks to Kafka.
 */
@Service
public class ReserveInventoryService {

    /** Consumer group name and value of processed_events.consumer (AI-CONTEXT §6.1). */
    public static final String CONSUMER = "inventory-service";

    private static final Logger log = LoggerFactory.getLogger(ReserveInventoryService.class);

    private final ProcessedEventRepository processedEvents;
    private final StockRepository stockRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final Clock clock;

    public ReserveInventoryService(
            ProcessedEventRepository processedEvents,
            StockRepository stockRepository,
            OutboxEventRepository outboxEventRepository,
            Clock clock) {
        this.processedEvents = processedEvents;
        this.stockRepository = stockRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.clock = clock;
    }

    /**
     * @return the reservation result, or empty if this event was already processed (duplicate
     *     delivery, REQ-FUNC-016)
     */
    @Transactional
    public Optional<ReservationResult> handle(UUID eventId, ReservationRequest request) {
        Instant now = clock.instant();
        if (!processedEvents.claim(eventId, CONSUMER, now)) {
            log.info("Duplicate OrderCreated event ignored");
            return Optional.empty();
        }

        ReservationResult result = stockRepository.tryReserve(request);
        outboxEventRepository.save(outcomeEvent(request.orderId().toString(), result, now));
        log.info("Reservation outcome: {}", result);
        return Optional.of(result);
    }

    /** InventoryReserved / InventoryRejected contracts (AI-CONTEXT §6.2). */
    private static OutboxEvent outcomeEvent(String orderId, ReservationResult result, Instant now) {
        return switch (result) {
            case ReservationResult.Reserved reserved ->
                    new OutboxEvent(
                            UUID.randomUUID(),
                            InventoryEvents.INVENTORY_RESERVED,
                            orderId,
                            InventoryEvents.INVENTORY_RESERVED_TOPIC,
                            Map.of("orderId", orderId),
                            now);
            case ReservationResult.Rejected rejected ->
                    new OutboxEvent(
                            UUID.randomUUID(),
                            InventoryEvents.INVENTORY_REJECTED,
                            orderId,
                            InventoryEvents.INVENTORY_REJECTED_TOPIC,
                            Map.of("orderId", orderId, "reason", rejected.reason().name()),
                            now);
        };
    }
}
