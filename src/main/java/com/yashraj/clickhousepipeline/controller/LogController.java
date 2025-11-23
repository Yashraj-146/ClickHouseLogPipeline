package com.yashraj.clickhousepipeline.controller;

import com.yashraj.clickhousepipeline.dto.LogDTO;
import com.yashraj.clickhousepipeline.service.IngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/logs")
@RequiredArgsConstructor
@Validated
public class LogController {

    private final IngestionService ingestionService;

    @PostMapping
    public ResponseEntity<String> ingestSingle(@Valid @RequestBody LogDTO log) {
        ingestionService.ingest(log);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("ingested");
    }

    @PostMapping("/batch")
    public ResponseEntity<String> ingestBatch(@Valid @RequestBody List<@Valid LogDTO> logs) {
        ingestionService.ingest(logs);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("batch ingested");
    }
}
