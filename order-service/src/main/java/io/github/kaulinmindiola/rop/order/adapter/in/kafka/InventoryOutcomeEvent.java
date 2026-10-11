package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import io.github.kaulinmindiola.rop.order.application.InventoryOutcome;
import java.util.UUID;

/**
 * A validated InventoryReserved or InventoryRejected event.
 *
 * @param reason the rejection reason; {@code null} for InventoryReserved
 */
public record InventoryOutcomeEvent(
        UUID eventId, UUID orderId, InventoryOutcome outcome, String reason) {}
