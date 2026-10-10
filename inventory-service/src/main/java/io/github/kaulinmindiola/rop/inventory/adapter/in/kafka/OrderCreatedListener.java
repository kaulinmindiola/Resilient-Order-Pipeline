package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import io.github.kaulinmindiola.rop.inventory.LogFields;
import io.github.kaulinmindiola.rop.inventory.adapter.out.kafka.KafkaTopics;
import io.github.kaulinmindiola.rop.inventory.application.ReserveInventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes order.created (AI-CONTEXT §6.1). Failures propagate to the error handler, which retries
 * transient ones and sends exhausted or invalid messages to the dead letter topic (ADR-0004). The
 * offset is committed after this method returns, that is, after the database commit (ADR-0011).
 */
@Component
public class OrderCreatedListener {

    public static final String ID = "orderCreatedListener";

    private final OrderCreatedEventParser parser;
    private final ReserveInventoryService reserveInventoryService;

    public OrderCreatedListener(
            OrderCreatedEventParser parser, ReserveInventoryService reserveInventoryService) {
        this.parser = parser;
        this.reserveInventoryService = reserveInventoryService;
    }

    @KafkaListener(
            id = ID,
            topics = KafkaTopics.ORDER_CREATED,
            groupId = ReserveInventoryService.CONSUMER,
            autoStartup = "${messaging.listeners.auto-startup:true}")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        // The key is the orderId (ADR-0003): known even if the value cannot be parsed.
        MDC.put(LogFields.ORDER_ID, String.valueOf(record.key()));
        try {
            OrderCreatedEvent event = parser.parse(record.value());
            MDC.put(LogFields.EVENT_ID, event.eventId().toString());
            reserveInventoryService.handle(event.eventId(), event.request());
        } finally {
            MDC.remove(LogFields.EVENT_ID);
            MDC.remove(LogFields.ORDER_ID);
        }
    }
}
