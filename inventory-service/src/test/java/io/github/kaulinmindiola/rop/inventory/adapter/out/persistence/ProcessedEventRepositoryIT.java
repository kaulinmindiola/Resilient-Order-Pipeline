package io.github.kaulinmindiola.rop.inventory.adapter.out.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.kaulinmindiola.rop.inventory.AbstractIntegrationTest;
import io.github.kaulinmindiola.rop.inventory.domain.port.ProcessedEventRepository;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

class ProcessedEventRepositoryIT extends AbstractIntegrationTest {

    private static final String CONSUMER = "test-consumer";
    private static final Instant NOW = Instant.parse("2026-01-15T10:00:00Z");

    @Autowired private ProcessedEventRepository processedEvents;
    @Autowired private TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("REQ-FUNC-016 the first claim succeeds and a repeated claim is a duplicate")
    void secondClaimIsDuplicate() {
        UUID eventId = UUID.randomUUID();

        assertThat(processedEvents.claim(eventId, CONSUMER, NOW)).isTrue();
        assertThat(processedEvents.claim(eventId, CONSUMER, NOW)).isFalse();
    }

    @Test
    @DisplayName("REQ-FUNC-016 idempotency is scoped per consumer")
    void sameEventForAnotherConsumerIsNotDuplicate() {
        UUID eventId = UUID.randomUUID();

        assertThat(processedEvents.claim(eventId, CONSUMER, NOW)).isTrue();
        assertThat(processedEvents.claim(eventId, "another-consumer", NOW)).isTrue();
    }

    @Test
    @DisplayName(
            "ADR-0011 a concurrent claim waits for the first transaction and then is a duplicate")
    void concurrentClaimIsResolvedByThePrimaryKey() throws Exception {
        UUID eventId = UUID.randomUUID();
        CountDownLatch firstClaimed = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first =
                    executor.submit(
                            () ->
                                    transactionTemplate.execute(
                                            status -> {
                                                boolean claimed =
                                                        processedEvents.claim(
                                                                eventId, CONSUMER, NOW);
                                                firstClaimed.countDown();
                                                awaitQuietly(releaseFirst);
                                                return claimed;
                                            }));
            assertThat(firstClaimed.await(10, TimeUnit.SECONDS)).isTrue();

            Future<Boolean> second =
                    executor.submit(
                            () ->
                                    transactionTemplate.execute(
                                            status ->
                                                    processedEvents.claim(eventId, CONSUMER, NOW)));

            // The second INSERT is blocked on the first, still uncommitted, row.
            assertThatThrownBy(() -> second.get(500, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            releaseFirst.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(10, TimeUnit.SECONDS)).isFalse();
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
