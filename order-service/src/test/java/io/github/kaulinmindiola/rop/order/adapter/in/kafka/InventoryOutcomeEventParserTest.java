package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.kaulinmindiola.rop.order.application.InventoryOutcome;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;

class InventoryOutcomeEventParserTest {

    private static final String EVENT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String ORDER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    private final InventoryOutcomeEventParser parser =
            new InventoryOutcomeEventParser(JsonMapper.builder().build());

    private static String envelope(String eventType, String aggregateId, String payload) {
        return """
               {"eventId":"%s","eventType":"%s","occurredAt":"2026-01-15T10:00:00.123Z",
                "aggregateId":"%s","payload":%s}
               """
                .formatted(EVENT_ID, eventType, aggregateId, payload);
    }

    @Test
    @DisplayName(
            "REQ-MAINT-002 a valid InventoryReserved becomes a RESERVED outcome without reason")
    void parsesReserved() {
        InventoryOutcomeEvent event =
                parser.parse(
                        envelope(
                                "InventoryReserved",
                                ORDER_ID,
                                "{\"orderId\":\"" + ORDER_ID + "\",\"futureField\":1}"),
                        InventoryOutcome.RESERVED);

        assertThat(event)
                .isEqualTo(
                        new InventoryOutcomeEvent(
                                UUID.fromString(EVENT_ID),
                                UUID.fromString(ORDER_ID),
                                InventoryOutcome.RESERVED,
                                null));
    }

    @Test
    @DisplayName(
            "REQ-MAINT-002 a valid InventoryRejected becomes a REJECTED outcome with its reason")
    void parsesRejected() {
        InventoryOutcomeEvent event =
                parser.parse(
                        envelope(
                                "InventoryRejected",
                                ORDER_ID,
                                "{\"orderId\":\""
                                        + ORDER_ID
                                        + "\",\"reason\":\"UNKNOWN_PRODUCT\"}"),
                        InventoryOutcome.REJECTED);

        assertThat(event.outcome()).isEqualTo(InventoryOutcome.REJECTED);
        assertThat(event.reason()).isEqualTo("UNKNOWN_PRODUCT");
    }

    static Stream<Arguments> contractViolations() {
        String validPayload = "{\"orderId\":\"" + ORDER_ID + "\"}";
        return Stream.of(
                Arguments.of("not json", InventoryOutcome.RESERVED),
                Arguments.of("null", InventoryOutcome.RESERVED),
                // eventType does not match the topic: a rejection on inventory.reserved
                Arguments.of(
                        envelope(
                                "InventoryRejected",
                                ORDER_ID,
                                "{\"orderId\":\""
                                        + ORDER_ID
                                        + "\",\"reason\":\"UNKNOWN_PRODUCT\"}"),
                        InventoryOutcome.RESERVED),
                Arguments.of(
                        envelope("InventoryReserved", ORDER_ID, "null"), InventoryOutcome.RESERVED),
                Arguments.of(
                        envelope("InventoryReserved", "another-id", validPayload),
                        InventoryOutcome.RESERVED),
                Arguments.of(
                        envelope("InventoryReserved", "not-a-uuid", "{\"orderId\":\"not-a-uuid\"}"),
                        InventoryOutcome.RESERVED),
                // a rejection without reason, and with an unknown one
                Arguments.of(
                        envelope("InventoryRejected", ORDER_ID, validPayload),
                        InventoryOutcome.REJECTED),
                Arguments.of(
                        envelope(
                                "InventoryRejected",
                                ORDER_ID,
                                "{\"orderId\":\"" + ORDER_ID + "\",\"reason\":\"BECAUSE\"}"),
                        InventoryOutcome.REJECTED),
                Arguments.of(
                        "{\"eventType\":\"InventoryReserved\",\"aggregateId\":\""
                                + ORDER_ID
                                + "\",\"payload\":"
                                + validPayload
                                + "}",
                        InventoryOutcome.RESERVED));
    }

    @ParameterizedTest
    @MethodSource("contractViolations")
    @DisplayName("ADR-0004 a message that violates its contract is an InvalidEventException")
    void contractViolationsAreInvalidEvents(String json, InventoryOutcome expected) {
        assertThatThrownBy(() -> parser.parse(json, expected))
                .isInstanceOf(InvalidEventException.class);
    }
}
