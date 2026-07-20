package com.jobqueue.service;

import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Service for managing dead-lettered jobs: listing, replaying, and discarding.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DlqService {

    private final JobRepository jobRepository;

    /**
     * List all DEAD_LETTER jobs, paginated and sorted by the caller's
     * choice (typically by updatedAt DESC to see the most recent failures first).
     */
    @Transactional(readOnly = true)
    public Page<Job> listDeadLetterJobs(Pageable pageable) {
        return jobRepository.findByStatus(JobStatus.DEAD_LETTER, pageable);
    }

    /**
     * Replay a dead-lettered job: reset it back to PENDING with attempts=0
     * and scheduled_at=now() so it re-enters the normal processing pipeline.
     *
     * @param id the job to replay
     * @throws IllegalArgumentException if the job doesn't exist
     * @throws IllegalStateException    if the job is not in DEAD_LETTER status
     */
    @Transactional
    public Job replayJob(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));

        if (job.getStatus() != JobStatus.DEAD_LETTER) {
            throw new IllegalStateException(
                    "Only DEAD_LETTER jobs can be replayed. Current status: " + job.getStatus());
        }

        job.setStatus(JobStatus.PENDING);
        job.setAttempts(0);
        job.setScheduledAt(Instant.now());
        job.setLastError(null);
        job.setLockedBy(null);
        job.setLockedAt(null);

        Job saved = jobRepository.save(job);
        log.info("Replayed dead-lettered job {} (type={})", id, job.getJobType());
        return saved;
    }

    /**
     * Permanently discard a dead-lettered job by deleting it from the database.
     *
     * @param id the job to discard
     * @throws IllegalArgumentException if the job doesn't exist
     * @throws IllegalStateException    if the job is not in DEAD_LETTER status
     */
    @Transactional
    public void discardJob(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));

        if (job.getStatus() != JobStatus.DEAD_LETTER) {
            throw new IllegalStateException(
                    "Only DEAD_LETTER jobs can be discarded. Current status: " + job.getStatus());
        }

        jobRepository.delete(job);
        log.info("Discarded dead-lettered job {} (type={})", id, job.getJobType());
    }
}
