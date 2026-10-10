package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Outbox fake with the same "pending, oldest first" semantics as the JDBC adapter. */
final class InMemoryOutbox implements OutboxEventRepository {

    private final List<OutboxEvent> events = new ArrayList<>();
    private final Map<UUID, Instant> publishedAt = new HashMap<>();
    private boolean failNextMark;

    @Override
    public void save(OutboxEvent event) {
        events.add(event);
    }

    @Override
    public List<OutboxEvent> findPending(int limit) {
        return events.stream()
                .filter(event -> !publishedAt.containsKey(event.eventId()))
                .sorted(Comparator.comparing(OutboxEvent::occurredAt))
                .limit(limit)
                .toList();
    }

    @Override
    public void markPublished(UUID eventId, Instant at) {
        if (failNextMark) {
            failNextMark = false;
            throw new IllegalStateException("simulated failure while marking");
        }
        publishedAt.put(eventId, at);
    }

    void failNextMark() {
        failNextMark = true;
    }

    Instant publishedAt(UUID eventId) {
        return publishedAt.get(eventId);
    }
}
