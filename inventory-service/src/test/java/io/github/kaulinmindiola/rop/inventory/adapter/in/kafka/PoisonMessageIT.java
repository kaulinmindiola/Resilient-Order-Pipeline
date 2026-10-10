package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.kaulinmindiola.rop.inventory.application.ReserveInventoryService;
import io.github.kaulinmindiola.rop.inventory.support.TestKafkaConsumer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * Test 10: a message that keeps failing is retried with the documented backoff, then sent to the
 * dead letter topic, and the consumer moves on; an invalid message is never retried (ADR-0004,
 * REQ-REL-001).
 */
class PoisonMessageIT extends AbstractListenerIT {

    @MockitoBean private ReserveInventoryService reserveInventoryService;
    @MockitoSpyBean private OrderCreatedEventParser parser;

    private static List<Long> gapsInMillis(List<Long> nanoTimes) {
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < nanoTimes.size(); i++) {
            gaps.add((nanoTimes.get(i) - nanoTimes.get(i - 1)) / 1_000_000);
        }
        return gaps;
    }

    private static ConsumerRecord<String, String> deadLettered(String key) {
        try (TestKafkaConsumer consumer =
                new TestKafkaConsumer(KAFKA.getBootstrapServers(), "order.created.DLT")) {
            List<ConsumerRecord<String, String>> records =
                    consumer.poll(record -> key.equals(record.key()), 1, Duration.ofSeconds(30));
            assertThat(records).hasSize(1);
            return records.getFirst();
        }
    }

    @Test
    @DisplayName(
            "REQ-REL-001 ADR-0004 test 10: a failing message gets 4 attempts with 1/2/4 s backoff, goes to the DLT, and the next message is processed")
    void transientFailureIsRetriedThenDeadLettered() throws Exception {
        UUID poisonEvent = UUID.randomUUID();
        UUID poisonOrder = UUID.randomUUID();
        UUID healthyEvent = UUID.randomUUID();
        List<Long> attempts = new CopyOnWriteArrayList<>();
        Set<UUID> handled = ConcurrentHashMap.newKeySet();
        when(reserveInventoryService.handle(any(), any()))
                .thenAnswer(
                        invocation -> {
                            UUID eventId = invocation.getArgument(0);
                            if (poisonEvent.equals(eventId)) {
                                attempts.add(System.nanoTime());
                                throw new IllegalStateException("simulated transient failure");
                            }
                            handled.add(eventId);
                            return Optional.empty();
                        });
        String key = poisonOrder.toString();
        String poison = orderCreated(poisonEvent, poisonOrder, List.of(line("SKU-001", 1)));

        int partition = send(key, poison);
        send(key, orderCreated(healthyEvent, UUID.randomUUID(), List.of(line("SKU-001", 1))));

        // Same key, same partition: the healthy message is only reached after the poison one.
        await().atMost(PROCESSING_TIMEOUT).until(() -> handled.contains(healthyEvent));

        assertThat(attempts).hasSize(4);
        assertThat(gapsInMillis(attempts))
                .satisfiesExactly(
                        gap -> assertThat(gap).isBetween(900L, 2_500L),
                        gap -> assertThat(gap).isBetween(1_800L, 3_500L),
                        gap -> assertThat(gap).isBetween(3_600L, 5_500L));

        ConsumerRecord<String, String> deadLetter = deadLettered(key);
        assertThat(deadLetter.value()).isEqualTo(poison);
        assertThat(deadLetter.partition()).isEqualTo(partition);
    }

    @Test
    @DisplayName(
            "REQ-REL-001 ADR-0004 test 10: an invalid message goes to the DLT after a single attempt")
    void invalidMessageIsDeadLetteredWithoutRetries() throws Exception {
        String key = "invalid-" + UUID.randomUUID();
        String garbage = "this is not json";

        int partition = send(key, garbage);

        ConsumerRecord<String, String> deadLetter = deadLettered(key);
        assertThat(deadLetter.value()).isEqualTo(garbage);
        assertThat(deadLetter.partition()).isEqualTo(partition);
        verify(parser, times(1)).parse(garbage);
        verifyNoInteractions(reserveInventoryService);
    }
}
