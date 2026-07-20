package com.jobqueue.controller;

import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lightweight endpoint for a quick manual check of job counts by status,
 * directly from the database — no Prometheus required.
 */
@RestController
@RequiredArgsConstructor
public class MetricsController {

    private final JobRepository jobRepository;

    /**
     * Returns a JSON snapshot like:
     * <pre>
     * {
     *   "PENDING": 42,
     *   "RUNNING": 3,
     *   "SUCCEEDED": 1200,
     *   "FAILED": 7,
     *   "DEAD_LETTER": 2,
     *   "total": 1254
     * }
     * </pre>
     */
    @GetMapping("/metrics/summary")
    public ResponseEntity<Map<String, Long>> summary() {
        Map<String, Long> counts = new LinkedHashMap<>();
        long total = 0;

        for (JobStatus status : JobStatus.values()) {
            long count = jobRepository.countByStatus(status);
            counts.put(status.name(), count);
            total += count;
        }

        counts.put("total", total);
        return ResponseEntity.ok(counts);
    }
}
