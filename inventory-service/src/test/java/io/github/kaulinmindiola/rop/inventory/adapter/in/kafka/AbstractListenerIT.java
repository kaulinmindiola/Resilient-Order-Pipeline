package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Integration tests that go through the real order.created listener. The listener is started and
 * stopped around each test, so only one consumer of the group is active at a time even when Spring
 * keeps several test contexts alive (DI-17). Every test creates its own TEST- products (DI-18).
 */
public abstract class AbstractListenerIT extends AbstractIntegrationTest {

    protected static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(30);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Autowired protected KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired protected KafkaTemplate<String, String> kafkaTemplate;
    @Autowired protected JdbcClient jdbc;

    /**
     * Unique per test instance: "TEST-" + UUID = 41 characters. product_id is varchar(64), so a
     * suffix may use at most 23 characters.
     */
    protected final String prefix = "TEST-" + UUID.randomUUID();

    private int sentinels;

    @BeforeEach
    void startListener() {
        orderCreatedListener().start();
    }

    @AfterEach
    void stopListener() {
        orderCreatedListener().stop();
    }

    private MessageListenerContainer orderCreatedListener() {
        return Objects.requireNonNull(
                listenerRegistry.getListenerContainer(OrderCreatedListener.ID));
    }

    protected String product(String suffix, int quantity) {
        String productId = prefix + suffix;
        jdbc.sql("INSERT INTO stock (product_id, quantity) VALUES (?, ?)")
                .params(productId, quantity)
                .update();
        return productId;
    }

    protected int stockOf(String productId) {
        return jdbc.sql("SELECT quantity FROM stock WHERE product_id = ?")
                .param(productId)
                .query(Integer.class)
                .single();
    }

    protected int claimsOf(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
    }

    /** Outbox rows of an order as "eventType:reason" ("-" when there is no reason). */
    protected List<String> outboxOf(UUID orderId) {
        return jdbc.sql(
                        "SELECT event_type || ':' || COALESCE(payload->>'reason', '-')"
                                + " FROM outbox_events WHERE aggregate_id = ?")
                .param(orderId.toString())
                .query(String.class)
                .list();
    }

    protected static Map<String, Object> line(String productId, int quantity) {
        return Map.of("productId", productId, "quantity", quantity);
    }

    /** An OrderCreated envelope exactly as order-service publishes it (AI-CONTEXT §6.2). */
    protected static String orderCreated(
            UUID eventId, UUID orderId, List<Map<String, Object>> lines) {
        return JSON.writeValueAsString(
                Map.of(
                        "eventId", eventId.toString(),
                        "eventType", "OrderCreated",
                        "occurredAt", Instant.now().toString(),
                        "aggregateId", orderId.toString(),
                        "payload", Map.of("orderId", orderId.toString(), "items", lines)));
    }

    /** Sends to order.created and returns the partition the record landed on. */
    protected int send(String key, String value) throws Exception {
        return kafkaTemplate
                .send(KafkaTopics.ORDER_CREATED, key, value)
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata()
                .partition();
    }

    protected void awaitOutbox(UUID orderId, String... expected) {
        await().atMost(PROCESSING_TIMEOUT)
                .untilAsserted(() -> assertThat(outboxOf(orderId)).containsExactly(expected));
    }

    /**
     * Sends a valid sentinel with the same key, hence to the same partition, and waits until it is
     * processed. Kafka preserves order within a partition, so everything sent before with this key
     * has been processed too.
     */
    protected void awaitEverythingSentBefore(String key) throws Exception {
        String sentinelProduct = product("-sentinel-" + (++sentinels), 1);
        UUID sentinelOrder = UUID.randomUUID();
        send(
                key,
                orderCreated(UUID.randomUUID(), sentinelOrder, List.of(line(sentinelProduct, 1))));
        awaitOutbox(sentinelOrder, "InventoryReserved:-");
    }
}
