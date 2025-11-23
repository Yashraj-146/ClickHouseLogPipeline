package com.yashraj.clickhousepipeline.service;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class IngestionService {

    private final BatchWriter batchWriter;

    public void ingest(LogDTO log) {
        batchWriter.enqueue(log);
    }

    public void ingest(List<LogDTO> logs) {
        logs.forEach(batchWriter::enqueue);
    }
}
