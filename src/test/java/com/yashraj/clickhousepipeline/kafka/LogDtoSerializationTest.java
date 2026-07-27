package com.yashraj.clickhousepipeline.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the exact serializer/deserializer construction used by KafkaProducerConfig and
 * KafkaConsumerConfig, standalone and with no broker involved. This is where the project's
 * earlier LocalDateTime pitfall would resurface if the ObjectMapper wiring ever regressed -
 * without JavaTimeModule registered, LogDTO.timestamp fails to serialize at all.
 */
@DisplayName("LogDTO Kafka JSON (de)serialization")
class LogDtoSerializationTest {

    // Mirrors Boot's auto-configured ObjectMapper: JavaTimeModule registered, dates written
    // as ISO-8601 strings rather than numeric timestamps - the same bean KafkaProducerConfig
    // and KafkaConsumerConfig receive via dependency injection in the real application.
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private static LogDTO sampleLog() {
        return new LogDTO(LocalDateTime.of(2026, 1, 15, 9, 30, 45), "WARN", "disk usage high", "storage-service");
    }

    @Test
    @DisplayName("round-trips a LogDTO through JsonSerializer and JsonDeserializer, preserving LocalDateTime")
    void serializeThenDeserialize_preservesAllFieldsIncludingTimestamp() {
        try (JsonSerializer<LogDTO> serializer = new JsonSerializer<>(objectMapper);
             JsonDeserializer<LogDTO> deserializer = new JsonDeserializer<>(LogDTO.class, objectMapper, false)) {

            LogDTO original = sampleLog();

            byte[] bytes = serializer.serialize("logs", original);
            LogDTO roundTripped = deserializer.deserialize("logs", bytes);

            assertThat(roundTripped).isEqualTo(original);
            assertThat(roundTripped.getTimestamp()).isEqualTo(original.getTimestamp());
        }
    }

    @Test
    @DisplayName("deserializes plain JSON bytes with no Kafka type headers")
    void deserialize_plainJsonWithNoTypeHeaders_producesLogDto() {
        try (JsonDeserializer<LogDTO> deserializer = new JsonDeserializer<>(LogDTO.class, objectMapper, false)) {
            LogDTO original = sampleLog();
            byte[] plainJsonBytes;
            try {
                plainJsonBytes = objectMapper.writeValueAsBytes(original);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            LogDTO result = deserializer.deserialize("logs", plainJsonBytes);

            assertThat(result).isEqualTo(original);
        }
    }

    @Test
    @DisplayName("throws when the bytes are not valid JSON")
    void deserialize_malformedBytes_throws() {
        try (JsonDeserializer<LogDTO> deserializer = new JsonDeserializer<>(LogDTO.class, objectMapper, false)) {
            byte[] garbage = "not valid json {{{".getBytes(StandardCharsets.UTF_8);

            // KafkaConsumerConfig relies on exactly this failure mode: ErrorHandlingDeserializer
            // wraps this exception so a single bad message is logged and skipped rather than
            // stalling the consumer on the offset forever.
            assertThatThrownBy(() -> deserializer.deserialize("logs", garbage)).isInstanceOf(RuntimeException.class);
        }
    }
}
