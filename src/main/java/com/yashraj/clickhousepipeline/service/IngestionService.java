package com.yashraj.clickhousepipeline.service;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.kafka.LogProducer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Thin routing layer. Depending on pipeline.ingestion.mode, a log is either handed straight
 * to BatchWriter (direct mode) or published to Kafka for LogConsumer to pick up and hand to
 * BatchWriter itself (kafka mode). No persistence logic lives here in either case.
 *
 * LogProducer is only registered as a bean in kafka mode (see its @ConditionalOnProperty),
 * so it is injected via ObjectProvider rather than directly - a plain constructor
 * dependency would fail to start the app in direct mode.
 */
@Service
public class IngestionService {

    private final BatchWriter batchWriter;
    private final ObjectProvider<LogProducer> logProducerProvider;
    private final String mode;

    public IngestionService(BatchWriter batchWriter,
                             ObjectProvider<LogProducer> logProducerProvider,
                             @Value("${pipeline.ingestion.mode:direct}") String mode) {
        this.batchWriter = batchWriter;
        this.logProducerProvider = logProducerProvider;
        this.mode = mode;
    }

    public void ingest(LogDTO log) {
        if (isKafkaMode()) {
            logProducerProvider.getObject().send(log);
        } else {
            batchWriter.enqueue(log);
        }
    }

    public void ingest(List<LogDTO> logs) {
        logs.forEach(this::ingest);
    }

    private boolean isKafkaMode() {
        return "kafka".equalsIgnoreCase(mode);
    }
}
