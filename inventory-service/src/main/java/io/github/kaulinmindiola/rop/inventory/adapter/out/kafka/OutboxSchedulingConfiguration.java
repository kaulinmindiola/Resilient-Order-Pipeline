package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Runs the outbox publisher periodically. fixedDelay starts the next cycle only after the previous
 * one ended, so slow cycles (Kafka down) never overlap. Disabled in integration tests, where the
 * publisher is invoked explicitly (DI-17).
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(
        prefix = "outbox.publisher",
        name = "scheduling-enabled",
        havingValue = "true",
        matchIfMissing = true)
public class OutboxSchedulingConfiguration {

    private final OutboxPublisher outboxPublisher;

    public OutboxSchedulingConfiguration(OutboxPublisher outboxPublisher) {
        this.outboxPublisher = outboxPublisher;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.polling-interval-ms}")
    public void publishPendingEvents() {
        outboxPublisher.publishPending();
    }
}
