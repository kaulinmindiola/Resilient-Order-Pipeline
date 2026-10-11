package io.github.kaulinmindiola.rop.order.application;

import io.github.kaulinmindiola.rop.order.domain.model.Order;
import io.github.kaulinmindiola.rop.order.domain.model.TransitionResult;
import io.github.kaulinmindiola.rop.order.domain.port.OrderRepository;
import io.github.kaulinmindiola.rop.order.domain.port.ProcessedEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies an inventory outcome to its order in a single local transaction (ADR-0011): claim the
 * event, then confirm or reject the order (BR-004, BR-005). A duplicate, an unknown order and an
 * invalid transition are recorded no-ops, never errors (BR-014, REQ-FUNC-019). There is no outgoing
 * event: the order is the end of the pipeline.
 */
@Service
public class ApplyInventoryOutcomeService {

    /** Consumer group name and value of processed_events.consumer (AI-CONTEXT §6.1). */
    public static final String CONSUMER = "order-service";

    private static final Logger log = LoggerFactory.getLogger(ApplyInventoryOutcomeService.class);

    private final ProcessedEventRepository processedEvents;
    private final OrderRepository orderRepository;
    private final Clock clock;

    public ApplyInventoryOutcomeService(
            ProcessedEventRepository processedEvents,
            OrderRepository orderRepository,
            Clock clock) {
        this.processedEvents = processedEvents;
        this.orderRepository = orderRepository;
        this.clock = clock;
    }

    @Transactional
    public OutcomeHandling apply(UUID eventId, UUID orderId, InventoryOutcome outcome) {
        Instant now = clock.instant();
        if (!processedEvents.claim(eventId, CONSUMER, now)) {
            log.info("Duplicate inventory event ignored");
            return OutcomeHandling.DUPLICATE;
        }

        Optional<Order> found = orderRepository.findById(orderId);
        if (found.isEmpty()) {
            log.warn("Inventory outcome {} for an unknown order ignored", outcome);
            return OutcomeHandling.UNKNOWN_ORDER;
        }

        Order order = found.get();
        TransitionResult result =
                switch (outcome) {
                    case RESERVED -> order.confirm(now);
                    case REJECTED -> order.reject(now);
                };

        return switch (result) {
            case TransitionResult.Applied applied -> {
                orderRepository.save(order);
                log.info("Order moved from {} to {}", applied.from(), applied.to());
                yield OutcomeHandling.TRANSITION_APPLIED;
            }
            case TransitionResult.Ignored ignored -> {
                log.warn(
                        "Transition ignored: order is {}, inventory outcome requested {}",
                        ignored.current(),
                        ignored.requested());
                yield OutcomeHandling.TRANSITION_IGNORED;
            }
        };
    }
}
