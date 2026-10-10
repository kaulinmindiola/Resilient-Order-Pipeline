package io.github.kaulinmindiola.rop.inventory.adapter.out.persistence;

import io.github.kaulinmindiola.rop.inventory.domain.port.ProcessedEventRepository;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Consumer idempotency in a single statement (ADR-0011, REQ-FUNC-016). The primary key also
 * resolves concurrent deliveries: a second INSERT waits for the first transaction and then inserts
 * nothing.
 */
@Repository
public class JdbcProcessedEventRepository implements ProcessedEventRepository {

    private static final String CLAIM =
            """
        INSERT INTO processed_events (event_id, consumer, processed_at)
        VALUES (:eventId, :consumer, :processedAt)
        ON CONFLICT DO NOTHING
        """;

    private final JdbcClient jdbc;

    public JdbcProcessedEventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean claim(UUID eventId, String consumer, Instant processedAt) {
        int inserted =
                jdbc.sql(CLAIM)
                        .param("eventId", eventId)
                        .param("consumer", consumer)
                        .param("processedAt", processedAt.atOffset(ZoneOffset.UTC))
                        .update();
        return inserted == 1;
    }
}
