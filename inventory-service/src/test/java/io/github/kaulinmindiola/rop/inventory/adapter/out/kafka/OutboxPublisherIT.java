package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.domain.model.OutboxEvent;
import io.github.kaulinmindiola.rop.inventory.domain.port.OutboxEventRepository;
import io.github.kaulinmindiola.rop.inventory.support.TestKafkaConsumer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

/** ADR-0013: inventory-service publishes through its own outbox with the same guarantees. */
class OutboxPublisherIT extends AbstractIntegrationTest {

    @Autowired private OutboxEventRepository outbox;
    @Autowired private OutboxPublisher outboxPublisher;
    @Autowired private JdbcClient jdbc;

    private void drainOutbox() {
        while (outboxPublisher.publishPending() > 0) {
            // keep publishing until nothing is pending
        }
    }

    @Test
    @DisplayName(
            "ADR-0013 REQ-FUNC-020 an inventory outbox event is published keyed by orderId, then marked")
    void publishesInventoryEvent() {
        String orderId = UUID.randomUUID().toString();
        OutboxEvent event =
                new OutboxEvent(
                        UUID.randomUUID(),
                        "InventoryReserved",
                        orderId,
                        "inventory.reserved",
                        Map.of("orderId", orderId),
                        Instant.now());
        outbox.save(event);

        drainOutbox();

        try (TestKafkaConsumer consumer =
                new TestKafkaConsumer(KAFKA.getBootstrapServers(), "inventory.reserved")) {
            List<ConsumerRecord<String, String>> records =
                    consumer.poll(
                            record -> orderId.equals(record.key()), 1, Duration.ofSeconds(30));

            assertThat(records).hasSize(1);
            String envelope = records.getFirst().value();
            assertThat((String) JsonPath.read(envelope, "$.eventId"))
                    .isEqualTo(event.eventId().toString());
            assertThat((String) JsonPath.read(envelope, "$.eventType"))
                    .isEqualTo("InventoryReserved");
            assertThat((String) JsonPath.read(envelope, "$.payload.orderId")).isEqualTo(orderId);
        }

        OffsetDateTime publishedAt =
                jdbc.sql("SELECT published_at FROM outbox_events WHERE event_id = ?")
                        .param(event.eventId())
                        .query(OffsetDateTime.class)
                        .single();
        assertThat(publishedAt).isNotNull();
    }
}
