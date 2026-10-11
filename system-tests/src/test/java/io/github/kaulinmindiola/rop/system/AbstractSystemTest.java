package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.kaulinmindiola.rop.system.support.SystemApi;
import io.github.kaulinmindiola.rop.system.support.SystemDatabase;
import java.io.File;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Starts the real docker-compose.yml once per test run, building the images from source, and
 * exposes the system only through its public API and, for assertions, its database (DI-11).
 *
 * <p>Constructed with the compose file only, ComposeContainer runs in "local compose" mode: it uses
 * the docker compose binary of the machine, exactly as a developer or the CI runner would.
 *
 * <p>Compose publishes fixed ports (8080, 5432, 9094): run {@code docker compose down} before these
 * tests, or they will fail with a port conflict.
 */
public abstract class AbstractSystemTest {

    /** The same development values as .env.example; these tests never read your local .env. */
    protected static final Map<String, String> ENVIRONMENT =
            Map.of(
                    "POSTGRES_PASSWORD", "postgres-dev",
                    "ORDER_DB_PASSWORD", "order-dev",
                    "INVENTORY_DB_PASSWORD", "inventory-dev",
                    "SEED_CLIENT_ID", "demo-client",
                    "SEED_CLIENT_SECRET", "demo-secret-change-me",
                    "JWT_SIGNING_SECRET", "dev-only-jwt-signing-secret-change-me-0123456789");

    protected static final Duration PIPELINE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);

    protected static final ComposeContainer COMPOSE =
            new ComposeContainer(new File("../docker-compose.yml"))
                    .withBuild(true)
                    .withEnv(ENVIRONMENT)
                    .waitingFor(
                            "order-service-1",
                            Wait.forLogMessage(".*Started OrderServiceApplication.*\\n", 1)
                                    .withStartupTimeout(STARTUP_TIMEOUT))
                    .waitingFor(
                            "inventory-service-1",
                            Wait.forLogMessage(".*Started InventoryServiceApplication.*\\n", 1)
                                    .withStartupTimeout(STARTUP_TIMEOUT));

    static {
        COMPOSE.start();
    }

    protected static final SystemApi API = new SystemApi("http://localhost:8080");
    protected static final SystemDatabase DATABASE =
            new SystemDatabase(
                    "jdbc:postgresql://localhost:5432/rop",
                    "postgres",
                    ENVIRONMENT.get("POSTGRES_PASSWORD"));

    protected static String token() {
        return API.obtainToken(
                ENVIRONMENT.get("SEED_CLIENT_ID"), ENVIRONMENT.get("SEED_CLIENT_SECRET"));
    }

    /** Waits for the asynchronous pipeline: two outbox cycles plus two consumers. */
    protected static void awaitStatus(String token, String orderId, String status) {
        await().atMost(PIPELINE_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(API.statusOf(token, orderId)).isEqualTo(status));
    }
}
