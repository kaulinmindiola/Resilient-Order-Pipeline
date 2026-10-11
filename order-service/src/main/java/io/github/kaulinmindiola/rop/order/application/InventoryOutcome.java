package io.github.kaulinmindiola.rop.order.application;

/** What inventory-service decided for an order (InventoryReserved / InventoryRejected). */
public enum InventoryOutcome {
    RESERVED,
    REJECTED
}
