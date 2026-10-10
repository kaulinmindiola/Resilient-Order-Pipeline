package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

class EventEnvelopeMapperTest {

    private static final TypeReference<Map<String, Object>> ENVELOPE_TYPE =
            new TypeReference<>() {};

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final EventEnvelopeMapper mapper = new EventEnvelopeMapper(jsonMapper);

    @Test
    @DisplayName(
            "REQ-MAINT-002 an InventoryRejected envelope carries the contract fields and the reason")
    void buildsEnvelopeAccordingToContract() {
        UUID eventId = UUID.fromString("66666666-6666-6666-6666-666666666666");
        Map<String, Object> payload = Map.of("orderId", "order-a", "reason", "INSUFFICIENT_STOCK");
        OutboxEvent event =
                new OutboxEvent(
                        eventId,
                        "InventoryRejected",
                        "order-a",
                        "inventory.rejected",
                        payload,
                        Instant.parse("2026-01-15T10:00:00Z"));

        Map<String, Object> envelope =
                jsonMapper.readValue(mapper.toEnvelopeJson(event), ENVELOPE_TYPE);

        assertThat(envelope)
                .containsOnlyKeys("eventId", "eventType", "occurredAt", "aggregateId", "payload")
                .containsEntry("eventId", eventId.toString())
                .containsEntry("eventType", "InventoryRejected")
                .containsEntry("occurredAt", "2026-01-15T10:00:00Z")
                .containsEntry("aggregateId", "order-a")
                .containsEntry("payload", payload);
    }
}
