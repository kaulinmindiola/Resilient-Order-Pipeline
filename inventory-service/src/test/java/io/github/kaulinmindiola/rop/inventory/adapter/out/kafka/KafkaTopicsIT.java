package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** inventory-service creates its topics at startup, with the declared topology (ADR-0012). */
class KafkaTopicsIT extends AbstractIntegrationTest {

    private static final List<String> DECLARED_TOPICS =
            List.of(
                    "order.created",
                    "order.created.DLT",
                    "inventory.reserved",
                    "inventory.rejected");

    @Test
    @DisplayName("REQ-FUNC-018 ADR-0012 every declared topic exists with 3 partitions and RF 1")
    void declaredTopicsExistWithExpectedTopology() throws Exception {
        try (AdminClient admin =
                AdminClient.create(
                        Map.of(
                                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                KAFKA.getBootstrapServers()))) {
            Map<String, TopicDescription> topics =
                    admin.describeTopics(DECLARED_TOPICS).allTopicNames().get(10, TimeUnit.SECONDS);

            assertThat(topics).containsOnlyKeys(DECLARED_TOPICS);
            assertThat(topics.values())
                    .allSatisfy(
                            topic -> {
                                assertThat(topic.partitions()).hasSize(3);
                                assertThat(topic.partitions())
                                        .allSatisfy(
                                                partition ->
                                                        assertThat(partition.replicas())
                                                                .hasSize(1));
                            });
        }
    }
}
