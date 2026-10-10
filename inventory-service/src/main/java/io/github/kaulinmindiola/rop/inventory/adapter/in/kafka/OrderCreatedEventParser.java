package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.kaulinmindiola.rop.inventory.domain.model.InvalidReservationException;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationItem;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Parses and validates an OrderCreated envelope (AI-CONTEXT §6.2). Every contract violation becomes
 * an {@link InvalidEventException}, so the domain only ever receives valid input. Unknown fields
 * are ignored, which keeps the contract backward compatible.
 */
@Component
public class OrderCreatedEventParser {

    static final String EVENT_TYPE = "OrderCreated";

    private final JsonMapper jsonMapper;

    public OrderCreatedEventParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public OrderCreatedEvent parse(String json) {
        Envelope envelope = read(json);
        if (!EVENT_TYPE.equals(envelope.eventType())) {
            throw new InvalidEventException("Unexpected eventType: " + envelope.eventType());
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
        if (payload.items() == null) {
            throw new InvalidEventException("payload.items is required");
        }
        try {
            List<ReservationItem> items = payload.items().stream().map(this::item).toList();
            return new OrderCreatedEvent(eventId, new ReservationRequest(orderId, items));
        } catch (InvalidReservationException e) {
            throw new InvalidEventException("OrderCreated describes an invalid reservation", e);
        }
    }

    private Envelope read(String json) {
        try {
            Envelope envelope = jsonMapper.readValue(json, Envelope.class);
            if (envelope == null) {
                throw new InvalidEventException("Empty OrderCreated envelope");
            }
            return envelope;
        } catch (JacksonException e) {
            throw new InvalidEventException("Unreadable OrderCreated envelope", e);
        }
    }

    private ReservationItem item(Item item) {
        if (item == null || item.quantity() == null) {
            throw new InvalidEventException("Every item needs a productId and a quantity");
        }
        return new ReservationItem(item.productId(), item.quantity());
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
    record Payload(String orderId, List<Item> items) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Item(String productId, Integer quantity) {}
}
