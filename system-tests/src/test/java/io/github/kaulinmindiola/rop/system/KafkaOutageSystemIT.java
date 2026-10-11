package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.kaulinmindiola.rop.system.support.SystemApi;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 7, complete: with the whole system running and Kafka paused, orders are still accepted and
 * none is lost; once Kafka resumes, every order reaches its terminal status on its own, and stock
 * is decremented exactly once per order even if events were redelivered (REQ-FUNC-011, ADR-0001,
 * ADR-0011).
 */
class KafkaOutageSystemIT extends AbstractSystemTest {

    private static final int ORDERS = 3;

    /**
     * At least two cycles of the 5 s outbox publisher, each one failing against the paused broker.
     */
    private static final Duration OUTAGE = Duration.ofSeconds(12);

    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(120);

    private static final String PENDING_EVENTS =
            "SELECT count(*) FROM order_service.outbox_events"
                    + " WHERE aggregate_id = ANY (CAST(? AS text[])) AND published_at IS NULL";
    private static final String ALL_EVENTS =
            "SELECT count(*) FROM order_service.outbox_events"
                    + " WHERE aggregate_id = ANY (CAST(? AS text[]))";

    private boolean kafkaPaused;

    @AfterEach
    void resumeKafka() {
        if (kafkaPaused) {
            unpause("kafka");
            kafkaPaused = false;
        }
    }

    @Test
    @DisplayName(
            "REQ-FUNC-011 test 7: orders created while Kafka is down are not lost and complete on their own once it returns")
    void ordersSurviveKafkaOutage() {
        String token = token();
        int stockBefore = DATABASE.stockOf("SKU-001");

        pause("kafka");
        kafkaPaused = true;
        List<String> orderIds = new ArrayList<>();
        for (int i = 0; i < ORDERS; i++) {
            HttpResponse<String> created = API.createOrder(token, "SKU-001", 1);
            assertThat(created.statusCode()).isEqualTo(201);
            orderIds.add(SystemApi.orderIdOf(created));
        }
        String[] ids = orderIds.toArray(String[]::new);

        // During the outage nothing is lost: the events wait in the database.
        await().during(OUTAGE)
                .atMost(OUTAGE.plusSeconds(5))
                .until(() -> DATABASE.queryInt(PENDING_EVENTS, ids) == ORDERS);
        assertThat(orderIds)
                .allSatisfy(id -> assertThat(API.statusOf(token, id)).isEqualTo("PENDING"));

        unpause("kafka");
        kafkaPaused = false;

        orderIds.forEach(id -> awaitStatus(token, id, "CONFIRMED", RECOVERY_TIMEOUT));
        assertThat(DATABASE.queryInt(PENDING_EVENTS, ids)).isZero();
        assertThat(DATABASE.queryInt(ALL_EVENTS, ids)).isEqualTo(ORDERS);
        assertThat(stockBefore - DATABASE.stockOf("SKU-001")).isEqualTo(ORDERS);
    }
}
