package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Test 8: at-least-once delivery, exactly-once effect (REQ-FUNC-016, BR-007, ADR-0011). */
class DuplicateDeliveryIT extends AbstractListenerIT {

    @Test
    @DisplayName("REQ-FUNC-016 BR-007 test 8: the same event delivered twice reserves stock once")
    void duplicateDeliveryHasASingleEffect() throws Exception {
        String productId = product("-a", 5);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String key = orderId.toString();
        String message = orderCreated(eventId, orderId, List.of(line(productId, 2)));

        send(key, message);
        send(key, message);
        awaitEverythingSentBefore(key);

        assertThat(stockOf(productId)).isEqualTo(3);
        assertThat(claimsOf(eventId)).isEqualTo(1);
        assertThat(outboxOf(orderId)).containsExactly("InventoryReserved:-");
    }
}
