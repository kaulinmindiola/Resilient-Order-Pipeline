package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import io.github.kaulinmindiola.rop.order.adapter.out.kafka.KafkaTopics;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Test 4: inventory outcomes drive the order to its terminal status, as seen through the API. */
class InventoryOutcomeConsumerIT extends AbstractInventoryOutcomeListenerIT {

    @Test
    @DisplayName(
            "BR-004 REQ-FUNC-014 test 4: InventoryReserved confirms the order (GET returns CONFIRMED)")
    void reservedConfirmsTheOrder() throws Exception {
        UUID orderId = pendingOrder();

        send(
                KafkaTopics.INVENTORY_RESERVED,
                orderId.toString(),
                inventoryReserved(UUID.randomUUID(), orderId));

        awaitStatus(orderId, "CONFIRMED");
    }

    @Test
    @DisplayName(
            "BR-005 REQ-FUNC-015 test 4: InventoryRejected rejects the order (GET returns REJECTED)")
    void rejectedRejectsTheOrder() throws Exception {
        UUID orderId = pendingOrder();

        send(
                KafkaTopics.INVENTORY_REJECTED,
                orderId.toString(),
                inventoryRejected(UUID.randomUUID(), orderId, "INSUFFICIENT_STOCK"));

        awaitStatus(orderId, "REJECTED");
    }
}
