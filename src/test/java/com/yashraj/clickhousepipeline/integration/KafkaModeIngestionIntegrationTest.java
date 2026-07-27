package com.yashraj.clickhousepipeline.integration;

import com.yashraj.clickhousepipeline.kafka.LogConsumer;
import com.yashraj.clickhousepipeline.kafka.LogProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The only test in the suite that exercises the real Kafka chain end to end: HTTP request ->
 * LogProducer -> embedded broker -> JSON deserialization -> @KafkaListener LogConsumer ->
 * BatchWriter.enqueue(). @EmbeddedKafka runs a real in-JVM broker (KRaft-less, single node),
 * so this needs no Docker and no external Kafka - it is the integration-fidelity counterpart
 * to LogProducerTest / LogConsumerTest / LogDtoSerializationTest, which mock the broker away
 * entirely for speed.
 *
 * "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}" is the standard
 * spring-kafka-test pattern: the embedded broker publishes its actual address under that
 * property key before context refresh, so the property placeholder resolves to wherever
 * the broker actually started.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = "logs")
@TestPropertySource(properties = {
        "pipeline.ingestion.mode=kafka",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=kafka-mode-it-group",
        "pipeline.batch.size=2",
        "pipeline.batch.flush-interval-ms=100",
        "pipeline.batch.queue-capacity=1000"
})
@DisplayName("Kafka-mode ingestion (full Spring context + embedded broker)")
class KafkaModeIngestionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ApplicationContext applicationContext;

    @MockitoBean
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("LogProducer and LogConsumer beans exist in the context when mode=kafka")
    void context_kafkaMode_containsProducerAndConsumerBeans() {
        assertThat(applicationContext.getBeanNamesForType(LogProducer.class)).hasSize(1);
        assertThat(applicationContext.getBeanNamesForType(LogConsumer.class)).hasSize(1);
    }

    @Test
    @DisplayName("a POST to /logs flows through the real producer -> broker -> consumer chain and reaches BatchWriter")
    void postLog_kafkaMode_flowsThroughRealBrokerToBatchWriter() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType("application/json")
                        .content("""
                                {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"kafka mode integration test","service":"it"}
                                """))
                .andExpect(status().isAccepted());

        // Consumer group formation (join/sync) on a fresh embedded broker can take a few
        // seconds even without Docker in the picture, hence the longer timeout than the
        // direct-mode equivalent.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(jdbcTemplate)
                        .batchUpdate(anyString(), anyList(), anyInt(), any()));
    }
}
