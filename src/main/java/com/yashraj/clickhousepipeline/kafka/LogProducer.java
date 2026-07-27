package com.yashraj.clickhousepipeline.kafka;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka-mode transport. Publishes a LogDTO to the "logs" topic and returns immediately -
 * it never writes to ClickHouse itself. The message key is the service name so that logs
 * from the same service land on the same partition, preserving per-service ordering while
 * still spreading load across partitions.
 */
@Component
@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")
public class LogProducer {

    private static final Logger log = LoggerFactory.getLogger(LogProducer.class);

    private final KafkaTemplate<String, LogDTO> kafkaTemplate;
    private final String topic;

    public LogProducer(KafkaTemplate<String, LogDTO> kafkaTemplate,
                        @Value("${pipeline.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(LogDTO dto) {
        String key = dto.getService() == null ? "unknown" : dto.getService();

        kafkaTemplate.send(topic, key, dto).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish log to Kafka topic {}: {}", topic, dto.getMessage(), ex);
            }
        });
    }
}
