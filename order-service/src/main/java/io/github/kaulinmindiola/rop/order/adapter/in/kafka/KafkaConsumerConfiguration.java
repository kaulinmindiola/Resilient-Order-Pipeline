package io.github.kaulinmindiola.rop.order.adapter.in.kafka;

import io.github.kaulinmindiola.rop.order.adapter.out.kafka.KafkaTopics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Consumer failure policy (ADR-0004, REQ-REL-001): one delivery plus 3 retries with 1 s, 2 s and 4
 * s backoff; exhausted messages go to {@code <topic>.DLT} on the same partition and the consumer
 * moves on. {@link InvalidEventException} is not retried. The partition is blocked during the
 * backoff, about 7 s per failing message: a deliberate trade-off.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfiguration {

    static final int MAX_RETRIES = 3;
    static final long INITIAL_BACKOFF_MS = 1_000;
    static final double BACKOFF_MULTIPLIER = 2.0;
    static final long MAX_BACKOFF_MS = 4_000;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        record.topic() + KafkaTopics.DLT_SUFFIX,
                                        record.partition()));

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        backOff.setInitialInterval(INITIAL_BACKOFF_MS);
        backOff.setMultiplier(BACKOFF_MULTIPLIER);
        backOff.setMaxInterval(MAX_BACKOFF_MS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidEventException.class);
        return errorHandler;
    }
}
