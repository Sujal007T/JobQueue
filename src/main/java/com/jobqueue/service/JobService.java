package com.jobqueue.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final JobRepository jobRepository;

    @Transactional
    public Job submitJob(String jobType, JsonNode payload, int priority, Instant scheduledAt, int maxAttempts) {
        if (jobType == null || jobType.trim().isEmpty()) {
            throw new IllegalArgumentException("job_type is required");
        }

        Job job = Job.builder()
                .jobType(jobType)
                .payload(payload)
                .status(JobStatus.PENDING)
                .priority(priority)
                .scheduledAt(scheduledAt != null ? scheduledAt : Instant.now())
                .maxAttempts(maxAttempts > 0 ? maxAttempts : 5)
                .attempts(0)
                .build();

        return jobRepository.save(job);
    }

    @Transactional(readOnly = true)
    public Optional<Job> getJob(Long id) {
        return jobRepository.findById(id);
    }

    @Transactional(readOnly = true)
    public List<Job> getJobsByStatus(String statusStr) {
        if (statusStr == null || statusStr.trim().isEmpty()) {
            return jobRepository.findAll();
        }
        JobStatus status = JobStatus.valueOf(statusStr.toUpperCase());
        return jobRepository.findByStatus(status);
    }

    @Transactional
    public void cancelJob(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found with id: " + id));

        if (job.getStatus() != JobStatus.PENDING) {
            throw new IllegalStateException("Only PENDING jobs can be cancelled. Current status: " + job.getStatus());
        }

        jobRepository.delete(job);
    }

    /**
     * Claims a batch of PENDING jobs for the given worker.
     * Delegates to the repository's FOR UPDATE SKIP LOCKED implementation.
     *
     * @param workerId  unique identifier for the worker claiming jobs
     * @param batchSize maximum number of jobs to claim
     * @return list of jobs now in RUNNING status, owned by this worker
     */
    @Transactional
    public List<Job> claimJobs(String workerId, int batchSize) {
        return jobRepository.claimJobs(workerId, batchSize);
    }

    /**
     * Mark a job as successfully completed.
     * Clears the lock fields since the job is no longer owned by any worker.
     */
    @Transactional
    public void markSucceeded(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        job.setStatus(JobStatus.SUCCEEDED);
        job.setLockedBy(null);
        job.setLockedAt(null);
        jobRepository.save(job);
    }

    /**
     * Mark a job as failed with the given error message.
     * For now this is a simple FAILED transition; retry/dead-letter
     * logic will be layered on top in a subsequent step.
     */
    @Transactional
    public void markFailed(Long id, String error) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        job.setStatus(JobStatus.FAILED);
        job.setLastError(error);
        job.setLockedBy(null);
        job.setLockedAt(null);
        jobRepository.save(job);
    }

    /**
     * Reset RUNNING jobs with stale locks back to PENDING.
     * Called once at startup by {@link com.jobqueue.worker.StaleJobRecovery}.
     *
     * @param threshold jobs locked before this instant are considered abandoned
     * @return the number of jobs reset
     */
    @Transactional
    public int resetStaleJobs(Instant threshold) {
        return jobRepository.resetStaleRunningJobs(threshold);
    }
}
