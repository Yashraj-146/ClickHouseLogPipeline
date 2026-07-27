package com.yashraj.clickhousepipeline.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.service.IngestionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer slice test: only LogController is loaded, with IngestionService mocked out.
 * Nothing here touches BatchWriter, Kafka, or ClickHouse - it exists purely to verify
 * request validation and that a valid request is delegated correctly.
 */
@WebMvcTest(LogController.class)
@DisplayName("LogController")
class LogControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private IngestionService ingestionService;

    private static String validSingleJson() {
        return """
                {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"hello","service":"billing"}
                """;
    }

    @Test
    @DisplayName("POST /logs returns 202 and delegates the parsed log to IngestionService")
    void ingestSingle_validPayload_returns202AndDelegates() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType("application/json")
                        .content(validSingleJson()))
                .andExpect(status().isAccepted())
                .andExpect(content().string("ingested"));

        var captor = org.mockito.ArgumentCaptor.forClass(LogDTO.class);
        verify(ingestionService).ingest(captor.capture());
        LogDTO captured = captor.getValue();
        org.assertj.core.api.Assertions.assertThat(captured.getTimestamp())
                .isEqualTo(LocalDateTime.of(2026, 1, 15, 9, 30, 0));
        org.assertj.core.api.Assertions.assertThat(captured.getLevel()).isEqualTo("INFO");
        org.assertj.core.api.Assertions.assertThat(captured.getMessage()).isEqualTo("hello");
        org.assertj.core.api.Assertions.assertThat(captured.getService()).isEqualTo("billing");
    }

    @Test
    @DisplayName("POST /logs accepts a payload with no 'service' field, defaulting it to 'unknown'")
    void ingestSingle_missingService_defaultsToUnknown() throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType("application/json")
                        .content("""
                                {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"hello"}
                                """))
                .andExpect(status().isAccepted());

        var captor = org.mockito.ArgumentCaptor.forClass(LogDTO.class);
        verify(ingestionService).ingest(captor.capture());
        org.assertj.core.api.Assertions.assertThat(captor.getValue().getService()).isEqualTo("unknown");
    }

    @Test
    @DisplayName("POST /logs/batch returns 202 and delegates the full parsed list to IngestionService")
    void ingestBatch_validPayload_returns202AndDelegatesFullList() throws Exception {
        String body = """
                [
                  {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"one","service":"a"},
                  {"timestamp":"2026-01-15T09:31:00","level":"WARN","message":"two","service":"b"}
                ]
                """;

        mockMvc.perform(post("/logs/batch")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(content().string("batch ingested"));

        @SuppressWarnings("unchecked")
        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(ingestionService).ingest(captor.capture());
        org.assertj.core.api.Assertions.assertThat(captor.getValue()).hasSize(2);
    }

    @Test
    @DisplayName("POST /logs/batch accepts an empty array")
    void ingestBatch_emptyArray_returns202() throws Exception {
        mockMvc.perform(post("/logs/batch")
                        .contentType("application/json")
                        .content("[]"))
                .andExpect(status().isAccepted());

        verify(ingestionService).ingest(List.<LogDTO>of());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidSinglePayloads")
    @DisplayName("POST /logs rejects an invalid payload with 400 and never reaches IngestionService")
    void ingestSingle_invalidPayload_returns400AndDoesNotDelegate(String caseName, String body) throws Exception {
        mockMvc.perform(post("/logs")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(ingestionService);
    }

    static Stream<Arguments> invalidSinglePayloads() {
        return Stream.of(
                Arguments.of("missing timestamp",
                        """
                        {"level":"INFO","message":"m","service":"svc"}
                        """),
                Arguments.of("null timestamp",
                        """
                        {"timestamp":null,"level":"INFO","message":"m","service":"svc"}
                        """),
                Arguments.of("missing level",
                        """
                        {"timestamp":"2026-01-15T09:30:00","message":"m","service":"svc"}
                        """),
                Arguments.of("blank level",
                        """
                        {"timestamp":"2026-01-15T09:30:00","level":"","message":"m","service":"svc"}
                        """),
                Arguments.of("whitespace-only level",
                        """
                        {"timestamp":"2026-01-15T09:30:00","level":"   ","message":"m","service":"svc"}
                        """),
                Arguments.of("missing message",
                        """
                        {"timestamp":"2026-01-15T09:30:00","level":"INFO","service":"svc"}
                        """),
                Arguments.of("blank message",
                        """
                        {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"","service":"svc"}
                        """),
                Arguments.of("malformed JSON",
                        "{ this is not valid json "),
                Arguments.of("wrong timestamp type",
                        """
                        {"timestamp":"not-a-date","level":"INFO","message":"m","service":"svc"}
                        """)
        );
    }

    @Test
    @DisplayName("KNOWN GAP: POST /logs/batch surfaces an invalid element as an unhandled server error, not 400")
    void ingestBatch_listWithInvalidElement_currentlyThrowsUnhandledConstraintViolation() throws Exception {
        // `@Valid @RequestBody List<@Valid LogDTO> logs` combined with the class-level
        // @Validated on LogController runs cascaded element validation through Spring's
        // MethodValidationInterceptor, which throws a raw jakarta.validation.
        // ConstraintViolationException. Spring MVC's default exception resolver chain has
        // no mapping for that exception type (only for MethodArgumentNotValidException,
        // which is what a single-object @Valid @RequestBody throws) - so it is never turned
        // into a 400 response. It propagates unresolved, and MockMvc's TestDispatcherServlet
        // rethrows it out of perform() itself rather than producing any MvcResult at all - in
        // a real deployment this reaches the container as an uncaught exception (HTTP 500).
        //
        // This test intentionally documents the CURRENT (buggy) behavior rather than the
        // desired one. Fixing it - e.g. with an @ExceptionHandler(ConstraintViolationException)
        // on LogController - is a real production-code change outside "add tests", and is
        // called out separately rather than made silently here.
        String body = """
                [
                  {"timestamp":"2026-01-15T09:30:00","level":"INFO","message":"valid","service":"a"},
                  {"timestamp":"2026-01-15T09:31:00","level":"","message":"invalid - blank level","service":"b"}
                ]
                """;

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        mockMvc.perform(post("/logs/batch")
                                .contentType("application/json")
                                .content(body)))
                .as("an invalid batch element currently escapes as an unhandled exception, not a 400 response")
                .hasRootCauseInstanceOf(jakarta.validation.ConstraintViolationException.class);

        verifyNoInteractions(ingestionService);
    }
}
