package com.jobqueue.repository;

import com.jobqueue.model.Job;

import java.util.List;

/**
 * Custom repository interface for operations that require native SQL
 * beyond what Spring Data JPA's derived queries can express —
 * specifically, SELECT ... FOR UPDATE SKIP LOCKED.
 */
public interface JobRepositoryCustom {

    /**
     * Atomically claims a batch of eligible PENDING jobs for a specific worker.
     * <p>
     * This method selects up to {@code batchSize} jobs that are PENDING and
     * due for execution (scheduled_at <= now), locks them with FOR UPDATE
     * SKIP LOCKED, transitions them to RUNNING, and returns the claimed
     * entities — all within a single transaction.
     *
     * @param workerId  unique identifier for the worker claiming the jobs
     * @param batchSize maximum number of jobs to claim in this batch
     * @return the list of jobs that were claimed (now in RUNNING status)
     */
    List<Job> claimJobs(String workerId, int batchSize);
}
