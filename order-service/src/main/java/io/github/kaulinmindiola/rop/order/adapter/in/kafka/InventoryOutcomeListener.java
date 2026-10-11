package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import io.github.kaulinmindiola.rop.order.LogFields;
import io.github.kaulinmindiola.rop.order.adapter.out.kafka.KafkaTopics;
import io.github.kaulinmindiola.rop.order.application.ApplyInventoryOutcomeService;
import io.github.kaulinmindiola.rop.order.application.InventoryOutcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes inventory.reserved and inventory.rejected with a single container (AI-CONTEXT §6.1). The
 * expected outcome is derived from the topic, and the parser checks that the eventType agrees.
 * Failures propagate to the error handler (ADR-0004); the offset is committed after the database
 * commit (ADR-0011).
 */
@Component
public class InventoryOutcomeListener {

    public static final String ID = "inventoryOutcomeListener";

    private static final Logger log = LoggerFactory.getLogger(InventoryOutcomeListener.class);

    private final InventoryOutcomeEventParser parser;
    private final ApplyInventoryOutcomeService applyInventoryOutcomeService;

    public InventoryOutcomeListener(
            InventoryOutcomeEventParser parser,
            ApplyInventoryOutcomeService applyInventoryOutcomeService) {
        this.parser = parser;
        this.applyInventoryOutcomeService = applyInventoryOutcomeService;
    }

    @KafkaListener(
            id = ID,
            topics = {KafkaTopics.INVENTORY_RESERVED, KafkaTopics.INVENTORY_REJECTED},
            groupId = ApplyInventoryOutcomeService.CONSUMER,
            autoStartup = "${messaging.listeners.auto-startup:true}")
    public void onInventoryOutcome(ConsumerRecord<String, String> record) {
        // The key is the orderId (ADR-0003): known even if the value cannot be parsed.
        MDC.put(LogFields.ORDER_ID, String.valueOf(record.key()));
        try {
            InventoryOutcomeEvent event = parser.parse(record.value(), outcomeOf(record.topic()));
            MDC.put(LogFields.EVENT_ID, event.eventId().toString());
            if (event.reason() != null) {
                log.info("Inventory rejected the order: {}", event.reason());
            }
            applyInventoryOutcomeService.apply(event.eventId(), event.orderId(), event.outcome());
        } finally {
            MDC.remove(LogFields.EVENT_ID);
            MDC.remove(LogFields.ORDER_ID);
        }
    }

    private static InventoryOutcome outcomeOf(String topic) {
        return switch (topic) {
            case KafkaTopics.INVENTORY_RESERVED -> InventoryOutcome.RESERVED;
            case KafkaTopics.INVENTORY_REJECTED -> InventoryOutcome.REJECTED;
            default -> throw new InvalidEventException("Unexpected topic: " + topic);
        };
    }
}
