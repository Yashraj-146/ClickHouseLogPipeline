package com.yashraj.clickhousepipeline.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares the "logs" topic so it exists at application startup instead of relying on
 * broker auto-create. Only active when Kafka mode is enabled - direct mode never touches
 * Kafka at all, per pipeline.ingestion.mode.
 */
@Configuration
@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")
public class KafkaTopicConfig {

    @Value("${pipeline.kafka.topic}")
    private String topicName;

    @Value("${pipeline.kafka.partitions:3}")
    private int partitions;

    @Bean
    public KafkaAdmin.NewTopics logsTopic() {
        // Single-broker dev cluster -> replication factor 1.
        // 3 partitions pairs naturally with the consumer's concurrency = 3.
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(topicName)
                        .partitions(partitions)
                        .replicas(1)
                        .build()
        );
    }
}
