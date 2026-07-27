package com.yashraj.clickhousepipeline.kafka;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.service.BatchWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LogConsumer's entire contract is "call BatchWriter.enqueue() and do nothing else" - it
 * must never write to ClickHouse itself (see CLAUDE.md). The real @KafkaListener wiring is
 * exercised separately by KafkaModeIngestionIntegrationTest via @EmbeddedKafka.
 */
@DisplayName("LogConsumer")
class LogConsumerTest {

    private final BatchWriter batchWriter = mock(BatchWriter.class);
    private final LogConsumer consumer = new LogConsumer(batchWriter);

    private static LogDTO log(String message) {
        return new LogDTO(LocalDateTime.now(), "INFO", message, "svc");
    }

    @Test
    @DisplayName("hands every consumed message straight to BatchWriter.enqueue()")
    void consume_delegatesToBatchWriterEnqueue() {
        LogDTO dto = log("from kafka");
        when(batchWriter.enqueue(dto)).thenReturn(true);

        consumer.consume(dto);

        verify(batchWriter).enqueue(dto);
    }

    @Test
    @DisplayName("does not throw when BatchWriter reports the queue is full")
    void consume_whenBatchWriterQueueIsFull_doesNotThrow() {
        LogDTO dto = log("dropped");
        when(batchWriter.enqueue(dto)).thenReturn(false);

        assertThatCode(() -> consumer.consume(dto)).doesNotThrowAnyException();

        verify(batchWriter).enqueue(dto);
    }
}
