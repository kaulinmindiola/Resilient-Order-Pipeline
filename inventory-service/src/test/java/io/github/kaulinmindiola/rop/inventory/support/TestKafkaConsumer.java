package io.github.kaulinmindiola.rop.inventory.support;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Reads a topic from the beginning with a fresh consumer group, so every test sees every record and
 * filters the ones it produced.
 */
public final class TestKafkaConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public TestKafkaConsumer(String bootstrapServers, String topic) {
        this.consumer =
                new KafkaConsumer<>(
                        Map.<String, Object>of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "test-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest",
                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                                false),
                        new StringDeserializer(),
                        new StringDeserializer());
        consumer.subscribe(List.of(topic));
    }

    /** Polls until {@code expected} matching records arrived or the timeout elapsed. */
    public List<ConsumerRecord<String, String>> poll(
            Predicate<ConsumerRecord<String, String>> filter, int expected, Duration timeout) {
        return pollUntil(filter, matches -> matches.size() >= expected, timeout);
    }

    /**
     * Polls until the matching records satisfy {@code done} or the timeout elapsed. Returns the
     * matches in consumption order (guaranteed only within a partition).
     */
    public List<ConsumerRecord<String, String>> pollUntil(
            Predicate<ConsumerRecord<String, String>> filter,
            Predicate<List<ConsumerRecord<String, String>>> done,
            Duration timeout) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        while (!done.test(matches) && Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                if (filter.test(record)) {
                    matches.add(record);
                }
            }
        }
        return matches;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
