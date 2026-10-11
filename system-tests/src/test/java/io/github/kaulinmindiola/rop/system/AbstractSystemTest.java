package io.github.kaulinmindiola.rop.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import io.github.kaulinmindiola.rop.system.support.SystemApi;
import io.github.kaulinmindiola.rop.system.support.SystemDatabase;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.testcontainers.DockerClientFactory;
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
 * tests, or they will fail with a port conflict. For the same reason the stack is stopped
 * synchronously when the test JVM exits, instead of relying only on Ryuk's asynchronous cleanup.
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
        // Registered before start(): a partially started stack also releases its fixed host ports.
        // Ryuk's cleanup is asynchronous, so without this hook the next run could find them taken.
        Runtime.getRuntime().addShutdownHook(new Thread(COMPOSE::stop, "compose-down"));
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
        awaitStatus(token, orderId, status, PIPELINE_TIMEOUT);
    }

    protected static void awaitStatus(
            String token, String orderId, String status, Duration timeout) {
        await().atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(API.statusOf(token, orderId)).isEqualTo(status));
    }

    // --- Container control, through the labels Compose puts on every container --------------

    private static DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    /**
     * The id of the only container of a Compose service. Fails if there is more than one, which
     * means another Compose stack is running and the test could act on the wrong container.
     */
    protected static String containerIdOf(String service) {
        List<Container> containers =
                docker().listContainersCmd()
                        .withLabelFilter(Map.of("com.docker.compose.service", service))
                        .exec();
        assertThat(containers)
                .as(
                        "running containers of Compose service '%s' (run docker compose down first)",
                        service)
                .hasSize(1);
        return containers.getFirst().getId();
    }

    protected static void pause(String service) {
        docker().pauseContainerCmd(containerIdOf(service)).exec();
    }

    protected static void unpause(String service) {
        docker().unpauseContainerCmd(containerIdOf(service)).exec();
    }

    /**
     * Restarts a service with no grace period ({@code docker restart -t 0}): the process gets
     * SIGKILL immediately, as in a real crash. Nothing is shut down cleanly: open transactions are
     * rolled back by the database and uncommitted offsets are redelivered by Kafka.
     */
    protected static void crashAndRestart(String service) {
        docker().restartContainerCmd(containerIdOf(service)).withTimeout(0).exec();
    }
}
