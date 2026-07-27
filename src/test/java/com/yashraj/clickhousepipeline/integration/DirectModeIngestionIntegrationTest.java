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
 * Full Spring context, real HTTP request via MockMvc, real BatchWriter batching thread -
 * only JdbcTemplate is mocked. This proves the direct-mode wiring end to end and that
 * @ConditionalOnProperty genuinely keeps Kafka out of the context, not just that
 * IngestionService's own if/else branches correctly (that narrower claim is
 * IngestionServiceTest's job).
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "pipeline.ingestion.mode=direct",
        "pipeline.batch.size=2",
        "pipeline.batch.flush-interval-ms=100",
        "pipeline.batch.queue-capacity=1000"
})
@DisplayName("Direct-mode ingestion (full Spring context)")
class DirectModeIngestionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ApplicationContext applicationContext;

    @MockitoBean
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("no LogProducer or LogConsumer bean exists in the context when mode=direct")
    void context_directMode_containsNoKafkaBeans() {
        assertThat(applicationContext.getBeanNamesForType(LogProducer.class)).isEmpty();
        assertThat(applicationContext.getBeanNamesForType(LogConsumer.class)).isEmpty();
    }

    @Test
    @DisplayName("a POST to /logs reaches BatchWriter and is written via JdbcTemplate")
    void postLog_directMode_reachesBatchWriter() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType("application/json")
                        .content("""
                                {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"direct mode integration test","service":"it"}
                                """))
                .andExpect(status().isAccepted());

        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> verify(jdbcTemplate)
                        .batchUpdate(anyString(), anyList(), anyInt(), any()));
    }
}
