package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import java.util.UUID;

/** A validated OrderCreated event, ready for the use case. */
public record OrderCreatedEvent(UUID eventId, ReservationRequest request) {}
