package io.github.kaulinmindiola.rop.inventory.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationItem;
import io.github.kaulinmindiola.rop.inventory.domain.model.ReservationRequest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

class OrderCreatedEventParserTest {

    private static final String EVENT_ID = "88888888-8888-8888-8888-888888888888";
    private static final String ORDER_ID = "99999999-9999-9999-9999-999999999999";

    private final OrderCreatedEventParser parser =
            new OrderCreatedEventParser(JsonMapper.builder().build());

    @Test
    @DisplayName("REQ-MAINT-002 a valid OrderCreated envelope becomes a reservation request")
    void parsesValidEnvelope() {
        String json =
                """
            {"eventId":"%s","eventType":"OrderCreated","occurredAt":"2026-01-15T10:00:00.123456Z",
             "aggregateId":"%s","futureField":"ignored",
             "payload":{"orderId":"%s","items":[{"productId":"SKU-001","quantity":2},
                                                 {"productId":"SKU-003","quantity":1}]}}
            """
                        .formatted(EVENT_ID, ORDER_ID, ORDER_ID);

        OrderCreatedEvent event = parser.parse(json);

        assertThat(event.eventId()).isEqualTo(UUID.fromString(EVENT_ID));
        assertThat(event.request())
                .isEqualTo(
                        new ReservationRequest(
                                UUID.fromString(ORDER_ID),
                                List.of(
                                        new ReservationItem("SKU-001", 2),
                                        new ReservationItem("SKU-003", 1))));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "not json",
                "null",
                "{\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":1}]}}",
                "{\"eventId\":\"not-a-uuid\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":1}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"SomethingElse\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":1}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\"}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\"another-id\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":1}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":0}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\"}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"quantity\":1}]}}",
                "{\"eventId\":\""
                        + EVENT_ID
                        + "\",\"eventType\":\"OrderCreated\",\"aggregateId\":\""
                        + ORDER_ID
                        + "\",\"payload\":{\"orderId\":\""
                        + ORDER_ID
                        + "\",\"items\":[{\"productId\":\"SKU-001\",\"quantity\":\"two\"}]}}"
            })
    @DisplayName(
            "ADR-0004 a message that violates the OrderCreated contract is an InvalidEventException")
    void contractViolationsAreInvalidEvents(String json) {
        assertThatThrownBy(() -> parser.parse(json)).isInstanceOf(InvalidEventException.class);
    }
}
