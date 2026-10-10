package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import static io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics.DLT_SUFFIX;
import static io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics.INVENTORY_REJECTED;
import static io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics.INVENTORY_RESERVED;
import static io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics.ORDER_CREATED;

import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares the topics this service consumes from or produces to, created at startup by KafkaAdmin
 * (REQ-FUNC-018, ADR-0012). Shared topics are declared with exactly the same definition as in
 * order-service, so whichever service starts first creates them identically.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfiguration {

    @Bean
    KafkaAdmin.NewTopics inventoryServiceTopics() {
        return new KafkaAdmin.NewTopics(
                Stream.of(
                                ORDER_CREATED,
                                ORDER_CREATED + DLT_SUFFIX,
                                INVENTORY_RESERVED,
                                INVENTORY_REJECTED)
                        .map(KafkaTopicsConfiguration::topic)
                        .toArray(NewTopic[]::new));
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(KafkaTopics.PARTITIONS)
                .replicas(KafkaTopics.REPLICAS)
                .build();
    }
}
