package com.yashraj.clickhousepipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer side of Kafka mode. Beans are named consumerFactory / kafkaListenerContainerFactory
 * so @KafkaListener picks them up without a containerFactory attribute.
 *
 * ErrorHandlingDeserializer wraps the JSON deserializer so a single malformed message on the
 * topic is logged and skipped instead of stalling the consumer forever on the same offset -
 * the classic beginner-Kafka trap. This is not a dead-letter queue (CLAUDE.md's "no DLQ"
 * limitation still applies); the bad record is simply dropped after logging.
 */
@Configuration
@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${pipeline.kafka.consumer.concurrency:3}")
    private int concurrency;

    @Bean
    public ConsumerFactory<String, LogDTO> consumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Spring commits the offset after the listener method returns successfully.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, LogDTO.class.getName());
        // Trust no type header from the message itself - always deserialize to LogDTO.
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        DefaultKafkaConsumerFactory<String, LogDTO> factory =
                new DefaultKafkaConsumerFactory<>(props);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogDTO> kafkaListenerContainerFactory(
            ConsumerFactory<String, LogDTO> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, LogDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
