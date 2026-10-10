package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.OutboxPublisher;
import io.github.kaulinmindiola.rop.inventory.support.TestKafkaConsumer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Test 3: an OrderCreated event leads to InventoryReserved or InventoryRejected on its topic. */
class OrderCreatedConsumerIT extends AbstractListenerIT {

    @Autowired private OutboxPublisher outboxPublisher;

    private void drainOutbox() {
        while (outboxPublisher.publishPending() > 0) {
            // keep publishing until nothing is pending
        }
    }

    private String publishedOutcome(String topic, UUID orderId) {
        drainOutbox();
        try (TestKafkaConsumer consumer =
                new TestKafkaConsumer(KAFKA.getBootstrapServers(), topic)) {
            List<ConsumerRecord<String, String>> records =
                    consumer.poll(
                            record -> orderId.toString().equals(record.key()),
                            1,
                            Duration.ofSeconds(30));
            assertThat(records).hasSize(1);
            return records.getFirst().value();
        }
    }

    @Test
    @DisplayName(
            "REQ-FUNC-012 test 3: OrderCreated with enough stock is reserved and InventoryReserved is published")
    void reservationIsPublished() throws Exception {
        String productId = product("-a", 5);
        UUID orderId = UUID.randomUUID();

        send(
                orderId.toString(),
                orderCreated(UUID.randomUUID(), orderId, List.of(line(productId, 2))));

        awaitOutbox(orderId, "InventoryReserved:-");
        assertThat(stockOf(productId)).isEqualTo(3);
        String envelope = publishedOutcome("inventory.reserved", orderId);
        assertThat((String) JsonPath.read(envelope, "$.eventType")).isEqualTo("InventoryReserved");
        assertThat((String) JsonPath.read(envelope, "$.payload.orderId"))
                .isEqualTo(orderId.toString());
    }

    @Test
    @DisplayName(
            "REQ-FUNC-013 test 3: OrderCreated without enough stock is rejected and InventoryRejected is published")
    void rejectionIsPublished() throws Exception {
        String productId = product("-a", 0);
        UUID orderId = UUID.randomUUID();

        send(
                orderId.toString(),
                orderCreated(UUID.randomUUID(), orderId, List.of(line(productId, 1))));

        awaitOutbox(orderId, "InventoryRejected:INSUFFICIENT_STOCK");
        String envelope = publishedOutcome("inventory.rejected", orderId);
        assertThat((String) JsonPath.read(envelope, "$.eventType")).isEqualTo("InventoryRejected");
        assertThat((String) JsonPath.read(envelope, "$.payload.reason"))
                .isEqualTo("INSUFFICIENT_STOCK");
    }
}
