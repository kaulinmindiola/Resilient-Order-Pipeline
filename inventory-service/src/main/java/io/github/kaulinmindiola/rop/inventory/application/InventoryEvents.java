package io.github.kaulinmindiola.rop.inventory.application;

/**
 * Event types written by this service and the topic each one is published to. The topic is resolved
 * when the event is written to the outbox (AI-CONTEXT §6.1).
 */
public final class InventoryEvents {

    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "InventoryRejected";
    public static final String INVENTORY_REJECTED_TOPIC = "inventory.rejected";

    private InventoryEvents() {}
}
