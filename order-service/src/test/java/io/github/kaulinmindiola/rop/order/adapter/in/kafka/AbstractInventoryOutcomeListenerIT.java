package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jayway.jsonpath.JsonPath;
import io.github.kaulinmindiola.rop.order.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.order.adapter.out.kafka.KafkaTopics;
import io.github.kaulinmindiola.rop.order.application.CreateOrderCommand;
import io.github.kaulinmindiola.rop.order.application.CreateOrderService;
import io.github.kaulinmindiola.rop.order.support.TestKafkaConsumer;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.json.JsonMapper;

/**
 * Integration tests that go through the real inventory outcome listener. The listener is started
 * and stopped around each test, so only one consumer of the group is active at a time (DI-17).
 * Order status is observed through the API, as a client would see it.
 */
public abstract class AbstractInventoryOutcomeListenerIT extends AbstractIntegrationTest {

    protected static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(30);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Autowired protected KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired protected KafkaTemplate<String, String> kafkaTemplate;
    @Autowired protected CreateOrderService createOrderService;
    @Autowired protected JdbcClient jdbc;

    private String token;

    @BeforeEach
    void startListener() {
        inventoryOutcomeListener().start();
    }

    @AfterEach
    void stopListener() {
        inventoryOutcomeListener().stop();
    }

    private MessageListenerContainer inventoryOutcomeListener() {
        return Objects.requireNonNull(
                listenerRegistry.getListenerContainer(InventoryOutcomeListener.ID));
    }

    /** One token per test, reused while polling: issuing one costs a BCrypt check. */
    private String token() {
        if (token == null) {
            token = bearerToken();
        }
        return token;
    }

    protected UUID pendingOrder() {
        return createOrderService
                .create(new CreateOrderCommand(List.of(new CreateOrderCommand.Item("SKU-001", 1))))
                .id();
    }

    protected String statusViaApi(UUID orderId) {
        MvcTestResult result =
                mvc.get()
                        .uri("/api/v1/orders/" + orderId)
                        .header(HttpHeaders.AUTHORIZATION, token())
                        .exchange();
        assertThat(result).hasStatus(HttpStatus.OK);
        try {
            return JsonPath.read(result.getResponse().getContentAsString(), "$.status");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void awaitStatus(UUID orderId, String status) {
        await().atMost(PROCESSING_TIMEOUT)
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(statusViaApi(orderId)).isEqualTo(status));
    }

    protected OffsetDateTime updatedAtOf(UUID orderId) {
        return jdbc.sql("SELECT updated_at FROM orders WHERE id = ?")
                .param(orderId)
                .query(OffsetDateTime.class)
                .single();
    }

    protected int claimsOf(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
    }

    /** InventoryReserved exactly as inventory-service publishes it (AI-CONTEXT §6.2). */
    protected static String inventoryReserved(UUID eventId, UUID orderId) {
        return envelope(
                eventId, "InventoryReserved", orderId, Map.of("orderId", orderId.toString()));
    }

    /** InventoryRejected exactly as inventory-service publishes it (AI-CONTEXT §6.2). */
    protected static String inventoryRejected(UUID eventId, UUID orderId, String reason) {
        return envelope(
                eventId,
                "InventoryRejected",
                orderId,
                Map.of("orderId", orderId.toString(), "reason", reason));
    }

    private static String envelope(
            UUID eventId, String eventType, UUID orderId, Map<String, Object> payload) {
        return JSON.writeValueAsString(
                Map.of(
                        "eventId", eventId.toString(),
                        "eventType", eventType,
                        "occurredAt", Instant.now().toString(),
                        "aggregateId", orderId.toString(),
                        "payload", payload));
    }

    /** Sends a record and returns the partition it landed on. */
    protected int send(String topic, String key, String value) throws Exception {
        return kafkaTemplate
                .send(topic, key, value)
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata()
                .partition();
    }

    /**
     * Sends a sentinel outcome for a fresh order with the same key to the same topic, hence the
     * same partition, and waits until that order is terminal. Kafka preserves order within a
     * partition, so everything sent before with this key on this topic has been processed too.
     */
    protected void awaitEverythingSentBefore(String topic, String key) throws Exception {
        UUID sentinel = pendingOrder();
        if (KafkaTopics.INVENTORY_RESERVED.equals(topic)) {
            send(topic, key, inventoryReserved(UUID.randomUUID(), sentinel));
            awaitStatus(sentinel, "CONFIRMED");
        } else {
            send(topic, key, inventoryRejected(UUID.randomUUID(), sentinel, "INSUFFICIENT_STOCK"));
            awaitStatus(sentinel, "REJECTED");
        }
    }

    protected static List<ConsumerRecord<String, String>> deadLettered(
            String topic, String key, Duration timeout) {
        try (TestKafkaConsumer consumer =
                new TestKafkaConsumer(
                        KAFKA.getBootstrapServers(), topic + KafkaTopics.DLT_SUFFIX)) {
            return consumer.poll(record -> key.equals(record.key()), 1, timeout);
        }
    }
}
