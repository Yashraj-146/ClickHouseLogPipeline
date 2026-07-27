package com.yashraj.clickhousepipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer side of Kafka mode. Beans are named producerFactory / kafkaTemplate so they
 * override Spring Boot's auto-configured defaults - no extra wiring needed elsewhere.
 *
 * The ObjectMapper here is the same Boot-managed bean the rest of the app uses (it already
 * has JavaTimeModule registered), so LogDTO's LocalDateTime timestamp serializes to ISO-8601
 * without a second hand-built mapper.
 */
@Configuration
@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, LogDTO> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Small producer-side linger to batch outgoing sends - the same batching idea
        // BatchWriter applies one layer down, just on the way into Kafka.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        DefaultKafkaProducerFactory<String, LogDTO> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, LogDTO> kafkaTemplate(ProducerFactory<String, LogDTO> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
