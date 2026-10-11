package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

/**
 * A message that violates its event contract. Deterministic, so it is never retried: it goes
 * straight to the dead letter topic (ADR-0004, REQ-REL-001).
 */
public class InvalidEventException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
