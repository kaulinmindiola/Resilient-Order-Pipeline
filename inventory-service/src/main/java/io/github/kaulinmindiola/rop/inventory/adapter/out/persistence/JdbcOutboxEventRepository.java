package io.github.kaulinmindiola.rop.inventory.adapter.out.persistence;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * JDBC implementation of the transactional outbox (ADR-0001, ADR-0018). The payload map is
 * serialized to jsonb here, so the domain never sees JSON (DI-14).
 */
@Repository
public class JdbcOutboxEventRepository implements OutboxEventRepository {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {};

    private static final String INSERT =
            """
        INSERT INTO outbox_events
            (event_id, event_type, aggregate_id, topic, payload, occurred_at)
        VALUES
            (:eventId, :eventType, :aggregateId, :topic, CAST(:payload AS jsonb), :occurredAt)
        """;

    private static final String FIND_PENDING =
            """
        SELECT event_id, event_type, aggregate_id, topic,
               CAST(payload AS text) AS payload, occurred_at
        FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY occurred_at
        LIMIT :limit
        """;

    private static final String MARK_PUBLISHED =
            "UPDATE outbox_events SET published_at = :publishedAt WHERE event_id = :eventId";

    private final JdbcClient jdbc;
    private final JsonMapper jsonMapper;

    public JdbcOutboxEventRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
        this.jdbc = jdbc;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void save(OutboxEvent event) {
        jdbc.sql(INSERT)
                .param("eventId", event.eventId())
                .param("eventType", event.eventType())
                .param("aggregateId", event.aggregateId())
                .param("topic", event.topic())
                .param("payload", jsonMapper.writeValueAsString(event.payload()))
                .param("occurredAt", utc(event.occurredAt()))
                .update();
    }

    @Override
    public List<OutboxEvent> findPending(int limit) {
        return jdbc.sql(FIND_PENDING)
                .param("limit", limit)
                .query(
                        (rs, rowNum) ->
                                new OutboxEvent(
                                        rs.getObject("event_id", UUID.class),
                                        rs.getString("event_type"),
                                        rs.getString("aggregate_id"),
                                        rs.getString("topic"),
                                        jsonMapper.readValue(rs.getString("payload"), PAYLOAD_TYPE),
                                        rs.getObject("occurred_at", OffsetDateTime.class)
                                                .toInstant()))
                .list();
    }

    @Override
    public void markPublished(UUID eventId, Instant publishedAt) {
        jdbc.sql(MARK_PUBLISHED)
                .param("publishedAt", utc(publishedAt))
                .param("eventId", eventId)
                .update();
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
