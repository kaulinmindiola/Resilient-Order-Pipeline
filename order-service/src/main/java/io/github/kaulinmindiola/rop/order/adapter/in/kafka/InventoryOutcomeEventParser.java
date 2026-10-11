package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.kaulinmindiola.rop.order.application.InventoryOutcome;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Parses and validates InventoryReserved / InventoryRejected envelopes (AI-CONTEXT §6.2). The
 * eventType must match the topic the record came from, and a rejection must carry a known reason.
 * Every contract violation becomes an {@link InvalidEventException}. Unknown fields are ignored.
 */
@Component
public class InventoryOutcomeEventParser {

    static final String INVENTORY_RESERVED = "InventoryReserved";
    static final String INVENTORY_REJECTED = "InventoryRejected";
    private static final Set<String> REJECTION_REASONS =
            Set.of("INSUFFICIENT_STOCK", "UNKNOWN_PRODUCT");

    private final JsonMapper jsonMapper;

    public InventoryOutcomeEventParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public InventoryOutcomeEvent parse(String json, InventoryOutcome expected) {
        Envelope envelope = read(json);
        String expectedType = eventTypeOf(expected);
        if (!expectedType.equals(envelope.eventType())) {
            throw new InvalidEventException(
                    "Expected " + expectedType + " but got " + envelope.eventType());
        }
        UUID eventId = uuid(envelope.eventId(), "eventId");
        Payload payload = envelope.payload();
        if (payload == null) {
            throw new InvalidEventException("payload is required");
        }
        UUID orderId = uuid(payload.orderId(), "payload.orderId");
        if (!payload.orderId().equals(envelope.aggregateId())) {
            throw new InvalidEventException("aggregateId does not match payload.orderId");
        }
        String reason = null;
        if (expected == InventoryOutcome.REJECTED) {
            if (payload.reason() == null || !REJECTION_REASONS.contains(payload.reason())) {
                throw new InvalidEventException("Unknown rejection reason: " + payload.reason());
            }
            reason = payload.reason();
        }
        return new InventoryOutcomeEvent(eventId, orderId, expected, reason);
    }

    static String eventTypeOf(InventoryOutcome outcome) {
        return switch (outcome) {
            case RESERVED -> INVENTORY_RESERVED;
            case REJECTED -> INVENTORY_REJECTED;
        };
    }

    private Envelope read(String json) {
        try {
            Envelope envelope = jsonMapper.readValue(json, Envelope.class);
            if (envelope == null) {
                throw new InvalidEventException("Empty inventory envelope");
            }
            return envelope;
        } catch (JacksonException e) {
            throw new InvalidEventException("Unreadable inventory envelope", e);
        }
    }

    private static UUID uuid(String value, String field) {
        if (value == null) {
            throw new InvalidEventException(field + " is required");
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException(field + " is not a UUID", e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Envelope(String eventId, String eventType, String aggregateId, Payload payload) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Payload(String orderId, String reason) {}
}
