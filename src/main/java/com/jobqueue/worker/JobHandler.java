package com.jobqueue.worker;

import com.jobqueue.model.Job;

/**
 * Functional interface for processing a specific type of job.
 * <p>
 * Implementations are registered in the {@link JobHandlerRegistry} keyed
 * by their job type string. The worker pool looks up the handler for each
 * claimed job and invokes {@link #handle(Job)}.
 * <p>
 * If the method returns normally, the job is marked SUCCEEDED.
 * If it throws, the job is handed to the failure-handling pipeline.
 */
@FunctionalInterface
public interface JobHandler {

    /**
     * Execute the job's business logic.
     *
     * @param job the claimed job (status=RUNNING) with its payload
     * @throws Exception if the job fails; the exception message is recorded as last_error
     */
    void handle(Job job) throws Exception;
}
