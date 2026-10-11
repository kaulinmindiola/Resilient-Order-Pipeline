package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jayway.jsonpath.JsonPath;
import io.github.kaulinmindiola.rop.system.support.SystemApi;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test 16: the real container logs, read as an operator would read them. An order can be followed
 * across both services by its orderId, event handling carries its eventId (REQ-OBS-001), and no
 * secret ever reaches a log (REQ-SEC-002).
 */
class StructuredLogsSystemIT extends AbstractSystemTest {

    private static final String UUID_PATTERN =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    /** JSON log lines of a service. Non-JSON output, if any, is ignored here. */
    private static List<Map<String, Object>> jsonLinesOf(String service) {
        return logsOf(service)
                .lines()
                .filter(line -> line.startsWith("{"))
                .map(line -> JsonPath.parse(line).<Map<String, Object>>read("$"))
                .toList();
    }

    private static List<Map<String, Object>> linesAbout(String service, String orderId) {
        return jsonLinesOf(service).stream()
                .filter(line -> orderId.equals(line.get("orderId")))
                .toList();
    }

    private static List<String> messagesOf(List<Map<String, Object>> lines) {
        return lines.stream().map(line -> String.valueOf(line.get("message"))).toList();
    }

    @Test
    @DisplayName(
            "REQ-OBS-001 test 16: every step of an order's journey is logged as JSON with its orderId, and event handling with its eventId")
    void orderJourneyIsTraceableAcrossServices() {
        String token = token();
        String orderId = SystemApi.orderIdOf(API.createOrder(token, "SKU-001", 1));
        awaitStatus(token, orderId, "CONFIRMED");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(
                        () -> {
                            assertThat(messagesOf(linesAbout("order-service", orderId)))
                                    .anySatisfy(m -> assertThat(m).startsWith("Order created"))
                                    .anySatisfy(
                                            m ->
                                                    assertThat(m)
                                                            .startsWith(
                                                                    "Outbox event OrderCreated published"))
                                    .anySatisfy(
                                            m ->
                                                    assertThat(m)
                                                            .isEqualTo(
                                                                    "Order moved from PENDING to CONFIRMED"));
                            assertThat(messagesOf(linesAbout("inventory-service", orderId)))
                                    .anySatisfy(
                                            m -> assertThat(m).startsWith("Reservation outcome"))
                                    .anySatisfy(
                                            m ->
                                                    assertThat(m)
                                                            .startsWith(
                                                                    "Outbox event InventoryReserved published"));
                        });

        List<Map<String, Object>> eventHandlingLines =
                Stream.concat(
                                linesAbout("order-service", orderId).stream(),
                                linesAbout("inventory-service", orderId).stream())
                        .filter(
                                line ->
                                        !String.valueOf(line.get("message"))
                                                .startsWith("Order created"))
                        .toList();
        assertThat(eventHandlingLines)
                .isNotEmpty()
                .allSatisfy(
                        line -> assertThat(line.get("eventId")).asString().matches(UUID_PATTERN));
    }

    @Test
    @DisplayName(
            "REQ-SEC-002 test 16: no log contains the access token, the client secret, the JWT key or any password")
    void logsContainNoSecrets() {
        String token = token();
        String orderId = SystemApi.orderIdOf(API.createOrder(token, "SKU-001", 1));
        awaitStatus(token, orderId, "CONFIRMED");

        List<String> secrets =
                ENVIRONMENT.entrySet().stream()
                        .filter(
                                e ->
                                        e.getKey().contains("PASSWORD")
                                                || e.getKey().contains("SECRET"))
                        .map(Map.Entry::getValue)
                        .toList();

        for (String service : List.of("order-service", "inventory-service")) {
            String logs = logsOf(service);
            assertThat(logs)
                    .as("logs of %s must not contain the issued access token", service)
                    .doesNotContain(token)
                    .doesNotContain("Bearer ey");
            assertThat(secrets)
                    .allSatisfy(
                            secret ->
                                    assertThat(logs)
                                            .as(
                                                    "logs of %s must not contain a configured secret",
                                                    service)
                                            .doesNotContain(secret));
        }
    }
}
