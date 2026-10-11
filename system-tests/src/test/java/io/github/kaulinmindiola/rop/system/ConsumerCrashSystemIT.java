package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.kaulinmindiola.rop.system.support.SystemApi;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 9: inventory-service is killed (SIGKILL) while it has work in flight. After it comes back,
 * no order is lost and no effect is duplicated, whatever was half-done at the moment of the crash
 * (ADR-0001, ADR-0011).
 */
class ConsumerCrashSystemIT extends AbstractSystemTest {

    private static final int ORDERS = 50;
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(180);

    /** OrderCreated events of these orders claimed by inventory-service. */
    private static final String CLAIMS =
            "SELECT count(*) FROM inventory_service.processed_events p"
                    + " JOIN order_service.outbox_events o ON o.event_id = p.event_id"
                    + " WHERE p.consumer = 'inventory-service'"
                    + " AND o.aggregate_id = ANY (CAST(? AS text[]))";

    private static final String PENDING_ORDERS =
            "SELECT count(*) FROM order_service.orders"
                    + " WHERE id = ANY (CAST(? AS uuid[])) AND status = 'PENDING'";
    private static final String INVENTORY_OUTCOMES =
            "SELECT count(*) FROM inventory_service.outbox_events"
                    + " WHERE aggregate_id = ANY (CAST(? AS text[]))";

    @Test
    @DisplayName(
            "ADR-0011 test 9: a consumer killed with work in flight neither loses nor duplicates any effect")
    void crashedConsumerNeitherLosesNorDuplicates() {
        String token = token();
        int stockBefore = DATABASE.stockOf("SKU-001");
        List<String> orderIds = new ArrayList<>();
        for (int i = 0; i < ORDERS; i++) {
            HttpResponse<String> created = API.createOrder(token, "SKU-001", 1);
            assertThat(created.statusCode()).isEqualTo(201);
            orderIds.add(SystemApi.orderIdOf(created));
        }
        String[] ids = orderIds.toArray(String[]::new);

        // Kill inventory-service as soon as it starts working on these orders.
        await().atMost(PIPELINE_TIMEOUT)
                .pollInterval(Duration.ofMillis(20))
                .until(() -> DATABASE.queryInt(CLAIMS, ids) > 0);
        crashAndRestart("inventory-service");
        assertThat(DATABASE.queryInt(PENDING_ORDERS, ids))
                .as("orders still in flight when inventory-service was killed")
                .isPositive();

        orderIds.forEach(id -> awaitStatus(token, id, "CONFIRMED", RECOVERY_TIMEOUT));

        assertThat(stockBefore - DATABASE.stockOf("SKU-001"))
                .as("stock decremented exactly once per order")
                .isEqualTo(ORDERS);
        assertThat(DATABASE.queryInt(CLAIMS, ids))
                .as("each OrderCreated claimed exactly once")
                .isEqualTo(ORDERS);
        assertThat(DATABASE.queryInt(INVENTORY_OUTCOMES, ids))
                .as("one InventoryReserved per order")
                .isEqualTo(ORDERS);
    }
}
