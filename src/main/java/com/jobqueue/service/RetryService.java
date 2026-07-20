package com.jobqueue.service;

import com.jobqueue.config.RetryConfigProperties;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Handles job failure with exponential backoff retry and dead-lettering.
 *
 * <h3>Retry formula</h3>
 * <pre>
 *   next_run = now() + min(base_delay × 2^attempts, max_delay) + jitter
 * </pre>
 * where:
 * <ul>
 *   <li>{@code base_delay} is configurable per job_type via
 *       {@link RetryConfigProperties}</li>
 *   <li>{@code attempts} is incremented BEFORE computing the delay</li>
 *   <li>{@code jitter} is a random value in [0, 20% of computed delay]
 *       to prevent thundering-herd retries when many jobs fail at the
 *       same time</li>
 *   <li>{@code max_delay} is a hard cap to prevent absurd wait times</li>
 * </ul>
 *
 * <h3>Dead-lettering</h3>
 * When {@code attempts >= max_attempts}, the job is moved to
 * {@link JobStatus#DEAD_LETTER} instead of being rescheduled.
 * Dead-lettered jobs remain in the database for inspection and manual
 * replay — they are never automatically retried.
 */
@Service
@Slf4j
public class RetryService {

    private final JobRepository jobRepository;
    private final RetryConfigProperties retryConfig;
    private final MeterRegistry meterRegistry;

    public RetryService(JobRepository jobRepository,
                        RetryConfigProperties retryConfig,
                        MeterRegistry meterRegistry) {
        this.jobRepository = jobRepository;
        this.retryConfig = retryConfig;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Handle a failed job execution. Called from the worker's catch block.
     *
     * <p>This method is transactional to ensure the attempt increment,
     * status change, and rescheduled_at are persisted atomically.</p>
     *
     * @param job the job that failed (in RUNNING status)
     * @param ex  the exception thrown by the handler
     */
    @Transactional
    public void handleFailure(Job job, Exception ex) {
        // Re-fetch within this transaction to ensure we operate on the
        // latest managed state, not a potentially detached entity from
        // the worker thread's earlier transaction.
        Job managed = jobRepository.findById(job.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Job disappeared while processing: " + job.getId()));

        // -----------------------------------------------------------------
        // Step 1: Record the failure
        // -----------------------------------------------------------------
        managed.setAttempts(managed.getAttempts() + 1);
        managed.setLastError(truncateError(ex));
        managed.setLockedBy(null);
        managed.setLockedAt(null);

        // -----------------------------------------------------------------
        // Step 2: Decide — retry or dead-letter
        // -----------------------------------------------------------------
        if (managed.getAttempts() >= managed.getMaxAttempts()) {
            // Exhausted all retries → dead-letter
            managed.setStatus(JobStatus.DEAD_LETTER);
            jobRepository.save(managed);

            // Record DLQ counter and final retry count
            Counter.builder("jobqueue.jobs.dlq")
                    .tag("job_type", managed.getJobType())
                    .register(meterRegistry)
                    .increment();
            DistributionSummary.builder("jobqueue.job.retries")
                    .tag("job_type", managed.getJobType())
                    .register(meterRegistry)
                    .record(managed.getAttempts());

            log.warn("Job {} moved to DEAD_LETTER after {}/{} attempts (type={}, error={})",
                     managed.getId(),
                     managed.getAttempts(),
                     managed.getMaxAttempts(),
                     managed.getJobType(),
                     ex.getMessage());
        } else {
            // Schedule retry with exponential backoff + jitter
            managed.setStatus(JobStatus.PENDING);

            long delayMs = computeDelay(managed.getJobType(), managed.getAttempts());
            Instant nextRun = Instant.now().plusMillis(delayMs);
            managed.setScheduledAt(nextRun);

            jobRepository.save(managed);

            log.info("Job {} scheduled for retry in {}ms (attempt {}/{}, type={}, error={})",
                     managed.getId(),
                     delayMs,
                     managed.getAttempts(),
                     managed.getMaxAttempts(),
                     managed.getJobType(),
                     ex.getMessage());
        }
    }

    // -----------------------------------------------------------------
    //  Delay computation
    // -----------------------------------------------------------------

    /**
     * Compute the retry delay using exponential backoff + jitter.
     *
     * <pre>
     *   exponential = base_delay × 2^attempts
     *   capped      = min(exponential, max_delay)
     *   jitter      = random in [0, capped × 0.20)
     *   total       = capped + jitter
     * </pre>
     *
     * The jitter is uniformly distributed across [0, 20%] of the
     * exponential delay. This staggers retries so that a batch of
     * simultaneously-failed jobs doesn't all hit the handler at
     * the exact same instant (thundering herd).
     *
     * @param jobType  used to look up the per-type base delay
     * @param attempts the attempt count AFTER incrementing (1-based)
     * @return total delay in milliseconds
     */
    long computeDelay(String jobType, int attempts) {
        long baseDelayMs = retryConfig.getBaseDelayMs(jobType);

        // 2^attempts via bit-shift. Safe for attempts < 63.
        long exponentialDelay = baseDelayMs * (1L << attempts);

        // Cap to prevent absurd delays when max_attempts is high
        long capped = Math.min(exponentialDelay, retryConfig.getMaxDelayMs());

        // Jitter: random value in [0, 20% of capped delay)
        // The +1 ensures nextLong's exclusive upper bound is at least 1
        long maxJitter = Math.max(1, (long) (capped * 0.20));
        long jitter = ThreadLocalRandom.current().nextLong(0, maxJitter + 1);

        return capped + jitter;
    }

    // -----------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------

    /**
     * Truncate the error message to fit the TEXT column without
     * producing obscenely large rows on deeply-nested stack traces.
     */
    private String truncateError(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            message = ex.getClass().getName();
        }
        return message.length() > 4000 ? message.substring(0, 4000) : message;
    }
}
