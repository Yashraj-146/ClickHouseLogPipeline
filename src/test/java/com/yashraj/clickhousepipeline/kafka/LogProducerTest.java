package com.yashraj.clickhousepipeline.kafka;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LogProducer only has one responsibility: publish to the configured topic with the right
 * key. The actual broker interaction is exercised separately by
 * KafkaModeIngestionIntegrationTest via @EmbeddedKafka - here KafkaTemplate is mocked so
 * these stay pure, fast unit tests.
 */
@DisplayName("LogProducer")
class LogProducerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, LogDTO> kafkaTemplate = mock(KafkaTemplate.class);

    private static LogDTO log(String message, String service) {
        return new LogDTO(LocalDateTime.now(), "INFO", message, service);
    }

    @Test
    @DisplayName("publishes to the configured topic keyed by the log's service")
    void send_publishesToConfiguredTopic_keyedByService() {
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        LogProducer producer = new LogProducer(kafkaTemplate, "logs");
        LogDTO dto = log("hello", "billing-service");

        producer.send(dto);

        verify(kafkaTemplate).send(eq("logs"), eq("billing-service"), eq(dto));
    }

    @Test
    @DisplayName("keys the message 'unknown' when the log has no service")
    void send_withNullService_usesUnknownAsKey() {
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        LogProducer producer = new LogProducer(kafkaTemplate, "logs");
        LogDTO dto = log("hello", null);

        producer.send(dto);

        verify(kafkaTemplate).send(eq("logs"), eq("unknown"), eq(dto));
    }

    @Test
    @DisplayName("logs a publish failure asynchronously instead of throwing from send()")
    void send_whenPublishFails_doesNotThrow() {
        CompletableFuture<SendResult<String, LogDTO>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);
        LogProducer producer = new LogProducer(kafkaTemplate, "logs");

        producer.send(log("hello", "svc"));
        // Simulate the broker call failing after send() has already returned control to
        // the caller. LogProducer's whenComplete callback must swallow this, not rethrow.
        future.completeExceptionally(new RuntimeException("broker unreachable"));

        // Reaching this line without an exception propagating out is the assertion.
    }
}
