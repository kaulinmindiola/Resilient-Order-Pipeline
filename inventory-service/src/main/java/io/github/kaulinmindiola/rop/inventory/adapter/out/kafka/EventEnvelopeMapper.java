package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Builds the event envelope published to Kafka. Values are plain strings (ADR-0004); the domain
 * never sees JSON.
 */
@Component
public class EventEnvelopeMapper {

    private final JsonMapper jsonMapper;

    public EventEnvelopeMapper(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public String toEnvelopeJson(OutboxEvent event) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", event.eventId().toString());
        envelope.put("eventType", event.eventType());
        envelope.put("occurredAt", event.occurredAt().toString());
        envelope.put("aggregateId", event.aggregateId());
        envelope.put("payload", event.payload());
        return jsonMapper.writeValueAsString(envelope);
    }
}
