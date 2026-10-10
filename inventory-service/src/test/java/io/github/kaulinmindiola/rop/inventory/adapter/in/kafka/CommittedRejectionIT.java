package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.support.TestKafkaConsumer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 14: a rejection is a business result, not a failure. It is committed with the claim and the
 * InventoryRejected event, decrements nothing, never reaches the DLT, and its redelivery is a no-op
 * (BR-015, ADR-0010, ADR-0011).
 */
class CommittedRejectionIT extends AbstractListenerIT {

    @Test
    @DisplayName(
            "BR-015 ADR-0010 ADR-0011 test 14: a partial rejection is committed with its claim, and its redelivery is a no-op")
    void partialRejectionIsCommittedAndRedeliveryIsANoOp() throws Exception {
        String available = product("-a", 5);
        String exhausted = product("-b", 0);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String key = orderId.toString();
        String message =
                orderCreated(eventId, orderId, List.of(line(available, 2), line(exhausted, 1)));

        send(key, message);
        awaitOutbox(orderId, "InventoryRejected:INSUFFICIENT_STOCK");
        assertThat(stockOf(available)).isEqualTo(5);
        assertThat(claimsOf(eventId)).isEqualTo(1);

        send(key, message);
        awaitEverythingSentBefore(key);
        assertThat(outboxOf(orderId)).containsExactly("InventoryRejected:INSUFFICIENT_STOCK");
        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(stockOf(available)).isEqualTo(5);

        try (TestKafkaConsumer dlt =
                new TestKafkaConsumer(KAFKA.getBootstrapServers(), "order.created.DLT")) {
            assertThat(dlt.poll(record -> key.equals(record.key()), 1, Duration.ofSeconds(3)))
                    .isEmpty();
        }
    }

    @Test
    @DisplayName(
            "REQ-FUNC-013 test 14: a product without a stock row is rejected with UNKNOWN_PRODUCT and nothing is decremented")
    void unknownProductIsCommittedAsRejection() throws Exception {
        String available = product("-a", 5);
        String unknown = prefix + "-z";
        UUID orderId = UUID.randomUUID();

        send(
                orderId.toString(),
                orderCreated(
                        UUID.randomUUID(), orderId, List.of(line(available, 1), line(unknown, 1))));

        awaitOutbox(orderId, "InventoryRejected:UNKNOWN_PRODUCT");
        assertThat(stockOf(available)).isEqualTo(5);
    }
}
