package com.yashraj.clickhousepipeline.service;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
public class BatchWriter {

    private static final Logger log = LoggerFactory.getLogger(BatchWriter.class);

    private final JdbcTemplate jdbcTemplate;

    private final int BATCH_SIZE = 1000;
    private final int FLUSH_INTERVAL_MS = 1000;

    private final BlockingQueue<LogDTO> queue = new LinkedBlockingQueue<>(100_000);

    private ScheduledExecutorService scheduler;
    private ExecutorService writerPool;
    private volatile boolean running = true;

    public boolean enqueue(LogDTO dto) {
        boolean offered = queue.offer(dto);

        if (!offered) {
            log.warn("Queue full - dropping log: {}", dto.getMessage());
        }

        if (queue.size() >= BATCH_SIZE) {
            scheduler.execute(this::flush);
        }

        return offered;
    }

    @PostConstruct
    public void start() {
        scheduler = Executors.newScheduledThreadPool(1);
        writerPool = Executors.newFixedThreadPool(2);

        scheduler.scheduleAtFixedRate(
                this::flush,
                FLUSH_INTERVAL_MS,
                FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        running = false;
        scheduler.shutdownNow();
        writerPool.shutdown();
        writerPool.awaitTermination(5, TimeUnit.SECONDS);
    }

    private void flush() {
        if (!running) return;

        List<LogDTO> batch = new ArrayList<>();
        queue.drainTo(batch, BATCH_SIZE);

        if (!batch.isEmpty()) {
            writerPool.execute(() -> writeBatch(batch));
        }
    }

    private void writeBatch(List<LogDTO> batch) {
        try {
            String sql = "INSERT INTO logs (timestamp, level, message, service) VALUES (?,?,?,?)";

            jdbcTemplate.batchUpdate(sql, batch, batch.size(), (ps, dto) -> {
                LocalDateTime ts = dto.getTimestamp();
                ps.setTimestamp(1, Timestamp.valueOf(ts));
                ps.setString(2, dto.getLevel());
                ps.setString(3, dto.getMessage());
                ps.setString(4, dto.getService() == null ? "unknown" : dto.getService());
            });

            log.info("Wrote {} logs to ClickHouse", batch.size());

        } catch (Exception ex) {
            log.error("Failed to write batch to ClickHouse", ex);
        }
    }
}
