package com.yashraj.clickhousepipeline.kafka;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.service.BatchWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka-mode transport, receiving side. This class NEVER writes to ClickHouse - it only
 * hands each LogDTO to BatchWriter.enqueue(), exactly like the direct-mode path does.
 * BatchWriter remains the single owner of all database writes, per CLAUDE.md.
 *
 * If BatchWriter's queue is full, enqueue() returns false and is dropped with a warning -
 * the same behavior direct mode already has. This is not a new backpressure mechanism;
 * consumer backpressure (e.g. pausing the container) remains out of scope, matching the
 * project's documented "no backpressure" limitation.
 */
@Component
@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")
public class LogConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);

    private final BatchWriter batchWriter;

    public LogConsumer(BatchWriter batchWriter) {
        this.batchWriter = batchWriter;
    }

    @KafkaListener(topics = "${pipeline.kafka.topic}")
    public void consume(LogDTO dto) {
        boolean accepted = batchWriter.enqueue(dto);
        if (!accepted) {
            log.warn("BatchWriter queue full - dropped log consumed from Kafka: {}", dto.getMessage());
        }
    }
}
