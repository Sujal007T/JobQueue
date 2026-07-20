package com.jobqueue.controller;

import com.jobqueue.dto.JobResponseDto;
import com.jobqueue.model.Job;
import com.jobqueue.service.DlqService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints for inspecting and managing the dead-letter queue.
 *
 * <ul>
 *   <li>{@code GET /jobs/dlq}          — list dead-lettered jobs (paginated)</li>
 *   <li>{@code POST /jobs/{id}/replay} — replay a dead-lettered job</li>
 *   <li>{@code DELETE /jobs/{id}/dlq}  — permanently discard a dead-lettered job</li>
 * </ul>
 */
@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class DlqController {

    private final DlqService dlqService;

    /**
     * List all DEAD_LETTER jobs, paginated.
     * Sorted by updatedAt DESC by default (most recent failures first).
     * The response includes last_error and attempts for failure inspection.
     *
     * @param page page number (0-based, default 0)
     * @param size page size (default 20)
     */
    @GetMapping("/dlq")
    public ResponseEntity<Page<JobResponseDto>> listDeadLetterJobs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "updatedAt"));
        Page<JobResponseDto> dtoPage = dlqService.listDeadLetterJobs(pageable)
                .map(JobResponseDto::fromEntity);

        return ResponseEntity.ok(dtoPage);
    }

    /**
     * Replay a dead-lettered job: reset to PENDING with attempts=0
     * and scheduled_at=now() for immediate re-processing.
     */
    @PostMapping("/{id}/replay")
    public ResponseEntity<JobResponseDto> replayJob(@PathVariable Long id) {
        try {
            Job replayed = dlqService.replayJob(id);
            return ResponseEntity.ok(JobResponseDto.fromEntity(replayed));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    /**
     * Permanently discard a dead-lettered job.
     */
    @DeleteMapping("/{id}/dlq")
    public ResponseEntity<Void> discardJob(@PathVariable Long id) {
        try {
            dlqService.discardJob(id);
            return ResponseEntity.noContent().build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }
}
