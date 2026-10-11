package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.order.adapter.out.kafka.KafkaTopics;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 12 and the other no-op rules: none of them is an error, so none of them is retried or
 * dead-lettered (BR-014, REQ-FUNC-016, REQ-FUNC-019). An invalid message, in contrast, goes
 * straight to the dead letter topic (ADR-0004).
 */
class InvalidTransitionIT extends AbstractInventoryOutcomeListenerIT {

    private static final Duration NOTHING_EXPECTED = Duration.ofSeconds(3);

    @Test
    @DisplayName(
            "BR-014 REQ-FUNC-019 test 12: InventoryReserved on a REJECTED order is a recorded no-op, not an error")
    void reservedOnRejectedOrderIsANoOp() throws Exception {
        UUID orderId = pendingOrder();
        String key = orderId.toString();
        send(
                KafkaTopics.INVENTORY_REJECTED,
                key,
                inventoryRejected(UUID.randomUUID(), orderId, "INSUFFICIENT_STOCK"));
        awaitStatus(orderId, "REJECTED");
        OffsetDateTime rejectedAt = updatedAtOf(orderId);
        UUID contradictoryEvent = UUID.randomUUID();

        send(KafkaTopics.INVENTORY_RESERVED, key, inventoryReserved(contradictoryEvent, orderId));
        awaitEverythingSentBefore(KafkaTopics.INVENTORY_RESERVED, key);

        assertThat(statusViaApi(orderId)).isEqualTo("REJECTED");
        assertThat(updatedAtOf(orderId)).isEqualTo(rejectedAt);
        assertThat(claimsOf(contradictoryEvent)).isEqualTo(1);
        assertThat(deadLettered(KafkaTopics.INVENTORY_RESERVED, key, NOTHING_EXPECTED)).isEmpty();
    }

    @Test
    @DisplayName("REQ-FUNC-019 an outcome for an unknown order is a recorded no-op, not an error")
    void unknownOrderIsANoOp() throws Exception {
        UUID unknownOrder = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String key = unknownOrder.toString();

        send(KafkaTopics.INVENTORY_RESERVED, key, inventoryReserved(eventId, unknownOrder));
        awaitEverythingSentBefore(KafkaTopics.INVENTORY_RESERVED, key);

        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(deadLettered(KafkaTopics.INVENTORY_RESERVED, key, NOTHING_EXPECTED)).isEmpty();
    }

    @Test
    @DisplayName(
            "REQ-FUNC-016 a duplicate outcome changes nothing: one transition, updated_at untouched")
    void duplicateOutcomeChangesNothing() throws Exception {
        UUID orderId = pendingOrder();
        UUID eventId = UUID.randomUUID();
        String key = orderId.toString();
        String message = inventoryReserved(eventId, orderId);
        send(KafkaTopics.INVENTORY_RESERVED, key, message);
        awaitStatus(orderId, "CONFIRMED");
        OffsetDateTime confirmedAt = updatedAtOf(orderId);

        send(KafkaTopics.INVENTORY_RESERVED, key, message);
        awaitEverythingSentBefore(KafkaTopics.INVENTORY_RESERVED, key);

        assertThat(updatedAtOf(orderId)).isEqualTo(confirmedAt);
        assertThat(claimsOf(eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName(
            "ADR-0004 an invalid inventory message goes straight to its DLT, on the same partition")
    void invalidMessageIsDeadLettered() throws Exception {
        String key = "invalid-" + UUID.randomUUID();
        String garbage = "this is not json";

        int partition = send(KafkaTopics.INVENTORY_REJECTED, key, garbage);

        List<ConsumerRecord<String, String>> deadLetters =
                deadLettered(KafkaTopics.INVENTORY_REJECTED, key, PROCESSING_TIMEOUT);
        assertThat(deadLetters).hasSize(1);
        assertThat(deadLetters.getFirst().value()).isEqualTo(garbage);
        assertThat(deadLetters.getFirst().partition()).isEqualTo(partition);
    }
}
