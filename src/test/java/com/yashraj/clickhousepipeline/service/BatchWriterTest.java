package com.yashraj.clickhousepipeline.service;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * BatchWriter is the heart of the pipeline (per CLAUDE.md), so it gets the deepest test
 * coverage. JdbcTemplate is mocked throughout - no test in this class ever touches a real
 * database. Batch size / flush interval / queue capacity are the constructor parameters
 * added specifically to make these tests deterministic and fast; production defaults
 * (1000 / 1000ms / 100000) are unchanged.
 */
@DisplayName("BatchWriter")
class BatchWriterTest {

    private JdbcTemplate jdbcTemplate;
    private final List<BatchWriter> writersToStop = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
    }

    @AfterEach
    void tearDown() {
        writersToStop.forEach(BatchWriterTest::stopQuietly);
        writersToStop.clear();
    }

    private BatchWriter newWriter(int batchSize, int flushIntervalMs, int queueCapacity) {
        BatchWriter writer = new BatchWriter(jdbcTemplate, batchSize, flushIntervalMs, queueCapacity);
        writer.start();
        writersToStop.add(writer);
        return writer;
    }

    private static LogDTO log(String message) {
        return new LogDTO(LocalDateTime.now(), "INFO", message, "svc");
    }

    private static void stopQuietly(BatchWriter writer) {
        try {
            writer.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Nested
    @DisplayName("enqueue")
    class Enqueue {

        @Test
        @DisplayName("returns true and accepts a log when the queue has capacity")
        void enqueue_withCapacity_returnsTrue() {
            BatchWriter writer = newWriter(1000, 60_000, 10);

            assertThat(writer.enqueue(log("hello"))).isTrue();
        }

        @Test
        @DisplayName("returns false and drops the log once the queue is full")
        void enqueue_whenQueueFull_returnsFalse() {
            // Flush interval far in the future so nothing drains the queue concurrently -
            // otherwise this assertion would be racy against the scheduler.
            BatchWriter writer = newWriter(1000, 60_000, 2);

            assertThat(writer.enqueue(log("one"))).isTrue();
            assertThat(writer.enqueue(log("two"))).isTrue();
            assertThat(writer.enqueue(log("three"))).isFalse();
        }
    }

    @Nested
    @DisplayName("flushing")
    class Flushing {

        @Test
        @DisplayName("flushes as soon as the queue reaches the configured batch size")
        void enqueue_reachingBatchSize_triggersImmediateFlush() {
            BatchWriter writer = newWriter(3, 60_000, 100);

            writer.enqueue(log("a"));
            writer.enqueue(log("b"));
            writer.enqueue(log("c"));

            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> verify(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(), any()));
        }

        @Test
        @DisplayName("flushes on the scheduled interval even when below the batch size")
        void scheduledFlush_belowBatchSize_stillWrites() {
            BatchWriter writer = newWriter(1000, 100, 100);

            writer.enqueue(log("solo"));

            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> verify(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(), any()));
        }

        @Test
        @DisplayName("never writes a batch larger than the configured batch size")
        void flush_neverExceedsConfiguredBatchSize() {
            int batchSize = 5;
            int totalLogs = 23;
            BatchWriter writer = newWriter(batchSize, 100, 1000);

            List<Integer> observedSizes = new ArrayList<>();
            doAnswer(inv -> {
                List<?> batch = inv.getArgument(1);
                synchronized (observedSizes) {
                    observedSizes.add(batch.size());
                }
                return new int[][]{};
            }).when(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(),
                    any(ParameterizedPreparedStatementSetter.class));

            for (int i = 0; i < totalLogs; i++) {
                writer.enqueue(log("m" + i));
            }

            await().atMost(Duration.ofSeconds(2)).until(() -> {
                synchronized (observedSizes) {
                    return observedSizes.stream().mapToInt(Integer::intValue).sum() >= totalLogs;
                }
            });

            synchronized (observedSizes) {
                assertThat(observedSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(batchSize));
            }
        }

        @Test
        @DisplayName("never calls batchUpdate while the queue stays empty")
        void scheduledFlush_onEmptyQueue_neverCallsBatchUpdate() {
            newWriter(1000, 50, 100);

            // Awaitility's during() polls the assertion for the whole window rather than
            // sleeping once and checking - it lets several scheduled flushes fire on the
            // empty queue without a raw Thread.sleep in the test. atMost is given generous
            // headroom over during() itself since it bounds polling overhead too, not just
            // the observation window.
            await().atMost(Duration.ofSeconds(2))
                    .during(Duration.ofMillis(300))
                    .untilAsserted(() -> verify(jdbcTemplate, never())
                            .batchUpdate(anyString(), anyList(), anyInt(), any()));
        }
    }

    @Nested
    @DisplayName("error handling")
    class ErrorHandling {

        @Test
        @DisplayName("swallows a JdbcTemplate exception and keeps writing subsequent batches")
        void writeBatch_whenJdbcTemplateThrows_doesNotStopSubsequentWrites() {
            BatchWriter writer = newWriter(1, 60_000, 100);
            doThrow(new RuntimeException("ClickHouse is down"))
                    .doReturn(new int[][]{})
                    .when(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(), any());

            writer.enqueue(log("first - triggers the failing batch"));
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> verify(jdbcTemplate, times(1))
                            .batchUpdate(anyString(), anyList(), anyInt(), any()));

            writer.enqueue(log("second - should still succeed"));
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> verify(jdbcTemplate, times(2))
                            .batchUpdate(anyString(), anyList(), anyInt(), any()));
        }

        @Test
        @DisplayName("maps DTO fields onto the PreparedStatement, defaulting a null service to 'unknown'")
        void writeBatch_mapsFieldsOntoPreparedStatement_defaultingNullServiceToUnknown() throws Exception {
            BatchWriter writer = newWriter(1, 60_000, 100);
            LocalDateTime ts = LocalDateTime.of(2026, 1, 1, 10, 30);
            LogDTO dto = new LogDTO(ts, "ERROR", "boom", null);

            writer.enqueue(dto);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<ParameterizedPreparedStatementSetter<LogDTO>> setterCaptor =
                    ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    verify(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(), setterCaptor.capture()));

            PreparedStatement ps = mock(PreparedStatement.class);
            setterCaptor.getValue().setValues(ps, dto);

            verify(ps).setTimestamp(1, Timestamp.valueOf(ts));
            verify(ps).setString(2, "ERROR");
            verify(ps).setString(3, "boom");
            verify(ps).setString(4, "unknown");
        }

        @Test
        @DisplayName("does not flush once stop() has been called")
        void flush_afterStop_isNoOp() throws Exception {
            BatchWriter writer = newWriter(1000, 60_000, 100);
            writer.enqueue(log("queued before stop"));
            writer.stop();

            // Invoke the private flush() reflectively to prove the `running` guard short-
            // circuits it on its own, independent of whether scheduler/writerPool are alive.
            Method flush = BatchWriter.class.getDeclaredMethod("flush");
            flush.setAccessible(true);
            flush.invoke(writer);

            verify(jdbcTemplate, never()).batchUpdate(anyString(), anyList(), anyInt(), any());
        }
    }

    @Nested
    @DisplayName("concurrency")
    class Concurrency {

        @Test
        @DisplayName("accepts logs from many concurrent producers with no loss or duplication")
        void enqueue_underConcurrentProducers_deliversEveryLogExactlyOnce() throws InterruptedException {
            int threadCount = 8;
            int perThread = 250;
            int total = threadCount * perThread;

            BatchWriter writer = newWriter(200, 50, total + 100);

            // Batches are copied into a thread-safe queue from inside the stub rather than
            // captured with an ArgumentCaptor: the List Mockito would capture is the very
            // list BatchWriter reuses/drains across concurrent invocations, so a captor is
            // not reliable here - the stub must copy defensively at call time.
            ConcurrentLinkedQueue<String> observedMessages = new ConcurrentLinkedQueue<>();
            doAnswer(inv -> {
                List<LogDTO> batch = inv.getArgument(1);
                batch.forEach(dto -> observedMessages.add(dto.getMessage()));
                return new int[][]{};
            }).when(jdbcTemplate).batchUpdate(anyString(), anyList(), anyInt(),
                    any(ParameterizedPreparedStatementSetter.class));

            ExecutorService producers = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threadCount);

            try {
                for (int t = 0; t < threadCount; t++) {
                    int threadIndex = t;
                    producers.submit(() -> {
                        ready.countDown();
                        try {
                            go.await();
                            for (int i = 0; i < perThread; i++) {
                                writer.enqueue(log("t" + threadIndex + "-m" + i));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }

                assertThat(ready.await(2, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

                await().atMost(Duration.ofSeconds(5)).until(() -> observedMessages.size() == total);

                Set<String> expected = IntStream.range(0, threadCount).boxed()
                        .flatMap(t -> IntStream.range(0, perThread).mapToObj(i -> "t" + t + "-m" + i))
                        .collect(Collectors.toSet());

                assertThat(observedMessages).hasSize(total);
                assertThat(new HashSet<>(observedMessages)).isEqualTo(expected);
            } finally {
                producers.shutdownNow();
            }
        }
    }
}
