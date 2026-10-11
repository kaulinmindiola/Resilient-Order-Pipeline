package io.github.kaulinmindiola.rop.order.application;

/** How an inventory outcome event was handled. None of them is an error (REQ-FUNC-019). */
public enum OutcomeHandling {
    /** The event had already been processed (REQ-FUNC-016). */
    DUPLICATE,
    /** No order exists with that id; the event is recorded as processed. */
    UNKNOWN_ORDER,
    /** The order moved to CONFIRMED or REJECTED (BR-004, BR-005). */
    TRANSITION_APPLIED,
    /** The order was already terminal; nothing changed (BR-014, BR-017). */
    TRANSITION_IGNORED
}
