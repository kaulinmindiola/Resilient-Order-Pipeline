package io.github.kaulinmindiola.rop.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import java.io.UnsupportedEncodingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Real PostgreSQL and Kafka shared by every integration test in this service (DI-05). The
 * application is configured through the same variables Docker Compose uses and connects with the
 * service role, never as superuser (ADR-0015). API tests authenticate exactly like a real client:
 * through POST /auth/token.
 *
 * <p>Neither the outbox publisher nor the Kafka listeners start on their own here: Spring keeps
 * several test contexts alive at once, and each would run its own publisher and join the same
 * consumer group. Tests drive them explicitly (DI-17).
 */
@SpringBootTest
@AutoConfigureMockMvc
public abstract class AbstractIntegrationTest {

    protected static final String DB_PASSWORD = "order-it";
    protected static final String INVENTORY_DB_PASSWORD = "inventory-it";
    protected static final String SEED_CLIENT_ID = "it-client";
    protected static final String SEED_CLIENT_SECRET = "it-secret";
    protected static final String JWT_SIGNING_SECRET = "it-jwt-signing-secret-0123456789-abcdef";

    protected static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:16")
                    .withEnv("ORDER_DB_PASSWORD", DB_PASSWORD)
                    .withEnv("INVENTORY_DB_PASSWORD", INVENTORY_DB_PASSWORD)
                    .withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    "../infra/postgres/init/01-schemas-and-roles.sh", 0755),
                            "/docker-entrypoint-initdb.d/01-schemas-and-roles.sh");

    protected static final KafkaContainer KAFKA =
            new KafkaContainer("apache/kafka:4.2.2")
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    @Autowired protected MockMvcTester mvc;

    @DynamicPropertySource
    static void environment(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", POSTGRES::getJdbcUrl);
        registry.add("DB_SCHEMA", () -> "order_service");
        registry.add("DB_USER", () -> "order_svc");
        registry.add("DB_PASSWORD", () -> DB_PASSWORD);
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registry.add("SEED_CLIENT_ID", () -> SEED_CLIENT_ID);
        registry.add("SEED_CLIENT_SECRET", () -> SEED_CLIENT_SECRET);
        registry.add("JWT_SIGNING_SECRET", () -> JWT_SIGNING_SECRET);
        registry.add("outbox.publisher.scheduling-enabled", () -> "false");
        registry.add("messaging.listeners.auto-startup", () -> "false");
    }

    /** Value for the Authorization header, obtained from the real token endpoint. */
    protected String bearerToken() {
        MvcTestResult result =
                mvc.post()
                        .uri("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(
                                """
                    {"clientId":"%s","clientSecret":"%s"}
                    """
                                        .formatted(SEED_CLIENT_ID, SEED_CLIENT_SECRET))
                        .exchange();
        assertThat(result).hasStatus(HttpStatus.OK);
        try {
            String accessToken =
                    JsonPath.read(result.getResponse().getContentAsString(), "$.accessToken");
            return "Bearer " + accessToken;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
