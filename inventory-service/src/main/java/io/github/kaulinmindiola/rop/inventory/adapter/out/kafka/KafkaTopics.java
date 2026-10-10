package io.github.kaulinmindiola.rop.inventory.adapter.out.kafka;

import io.github.kaulinmindiola.rop.inventory.application.InventoryEvents;

/** Topic names used by this service, defined once (AI-CONTEXT §6.1). */
public final class KafkaTopics {

    public static final String ORDER_CREATED = "order.created";
    public static final String INVENTORY_RESERVED = InventoryEvents.INVENTORY_RESERVED_TOPIC;
    public static final String INVENTORY_REJECTED = InventoryEvents.INVENTORY_REJECTED_TOPIC;
    public static final String DLT_SUFFIX = ".DLT";

    public static final int PARTITIONS = 3;
    public static final int REPLICAS = 1;

    private KafkaTopics() {}
}
