package com.jobqueue.controller;

import com.jobqueue.dto.JobRequestDto;
import com.jobqueue.dto.JobResponseDto;
import com.jobqueue.model.Job;
import com.jobqueue.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping
    public ResponseEntity<JobResponseDto> submitJob(@RequestBody JobRequestDto request) {
        if (request.getJobType() == null || request.getJobType().trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        int priority = request.getPriority() != null ? request.getPriority() : 0;
        int maxAttempts = request.getMaxAttempts() != null ? request.getMaxAttempts() : 5;
        Instant scheduledAt = request.getScheduledAt() != null ? request.getScheduledAt() : Instant.now();

        Job job = jobService.submitJob(
                request.getJobType(),
                request.getPayload(),
                priority,
                scheduledAt,
                maxAttempts
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(JobResponseDto.fromEntity(job));
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponseDto> getJob(@PathVariable Long id) {
        return jobService.getJob(id)
                .map(job -> ResponseEntity.ok(JobResponseDto.fromEntity(job)))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<JobResponseDto>> listJobs(@RequestParam(required = false) String status) {
        List<Job> jobs;
        try {
            jobs = jobService.getJobsByStatus(status);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build(); // Triggered if status enum is invalid
        }

        List<JobResponseDto> response = jobs.stream()
                .map(JobResponseDto::fromEntity)
                .collect(Collectors.toList());

        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> cancelJob(@PathVariable Long id) {
        try {
            jobService.cancelJob(id);
            return ResponseEntity.noContent().build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build(); // 409 Conflict when not PENDING
        }
    }
}
