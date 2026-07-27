package com.yashraj.clickhousepipeline.service;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.kafka.LogProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * IngestionService is a pure routing layer - these tests only assert *which* downstream
 * collaborator receives the log for a given pipeline.ingestion.mode. No Spring context is
 * needed since the mode is just a constructor string in production too.
 */
@DisplayName("IngestionService")
class IngestionServiceTest {

    private final BatchWriter batchWriter = mock(BatchWriter.class);
    private final LogProducer logProducer = mock(LogProducer.class);

    @SuppressWarnings("unchecked")
    private final ObjectProvider<LogProducer> logProducerProvider = mock(ObjectProvider.class);

    private static LogDTO log(String message) {
        return new LogDTO(LocalDateTime.now(), "INFO", message, "svc");
    }

    private IngestionService service(String mode) {
        return new IngestionService(batchWriter, logProducerProvider, mode);
    }

    @Test
    @DisplayName("direct mode sends a single log to BatchWriter and never touches the Kafka producer")
    void ingest_directMode_routesToBatchWriterOnly() {
        IngestionService service = service("direct");
        LogDTO dto = log("hello");

        service.ingest(dto);

        verify(batchWriter).enqueue(dto);
        verifyNoInteractions(logProducerProvider);
    }

    @Test
    @DisplayName("kafka mode sends a single log to the producer and never touches BatchWriter")
    void ingest_kafkaMode_routesToProducerOnly() {
        when(logProducerProvider.getObject()).thenReturn(logProducer);
        IngestionService service = service("kafka");
        LogDTO dto = log("hello");

        service.ingest(dto);

        verify(logProducer).send(dto);
        verifyNoInteractions(batchWriter);
    }

    @Test
    @DisplayName("mode matching is case-insensitive ('KAFKA' still routes to the producer)")
    void ingest_modeIsCaseInsensitive() {
        when(logProducerProvider.getObject()).thenReturn(logProducer);
        IngestionService service = service("KAFKA");
        LogDTO dto = log("hello");

        service.ingest(dto);

        verify(logProducer).send(dto);
        verifyNoInteractions(batchWriter);
    }

    @Test
    @DisplayName("an unrecognized mode falls back to the direct path")
    void ingest_unknownMode_fallsBackToDirect() {
        IngestionService service = service("banana");
        LogDTO dto = log("hello");

        service.ingest(dto);

        verify(batchWriter).enqueue(dto);
        verifyNoInteractions(logProducerProvider);
    }

    @Test
    @DisplayName("direct mode sends every element of a batch to BatchWriter")
    void ingestList_directMode_enqueuesEachLog() {
        IngestionService service = service("direct");
        List<LogDTO> logs = List.of(log("a"), log("b"), log("c"));

        service.ingest(logs);

        logs.forEach(dto -> verify(batchWriter).enqueue(dto));
        verifyNoInteractions(logProducerProvider);
    }

    @Test
    @DisplayName("kafka mode publishes every element of a batch through the producer")
    void ingestList_kafkaMode_publishesEachLog() {
        when(logProducerProvider.getObject()).thenReturn(logProducer);
        IngestionService service = service("kafka");
        List<LogDTO> logs = List.of(log("a"), log("b"), log("c"));

        service.ingest(logs);

        logs.forEach(dto -> verify(logProducer).send(dto));
        verify(logProducerProvider, times(3)).getObject();
        verifyNoInteractions(batchWriter);
    }

    @Test
    @DisplayName("an empty batch triggers no calls to either transport")
    void ingestList_emptyList_doesNothing() {
        IngestionService service = service("direct");

        service.ingest(Collections.<LogDTO>emptyList());

        verifyNoInteractions(batchWriter, logProducerProvider);
    }
}
