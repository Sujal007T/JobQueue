package com.jobqueue.worker;

import com.jobqueue.config.WorkerConfigProperties;
import com.jobqueue.model.Job;
import com.jobqueue.service.JobService;
import com.jobqueue.service.RetryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The core polling engine of the job queue.
 * <p>
 * On application startup (after stale job recovery), it spins up a
 * configurable number of worker threads. Each thread runs an independent
 * poll loop: claim a batch of jobs via {@link JobService#claimJobs},
 * dispatch each one to the appropriate {@link JobHandler}, and record
 * the outcome.
 * <p>
 * On shutdown (SIGTERM / @PreDestroy), the pool stops claiming new jobs,
 * waits for in-flight jobs to complete (up to a configurable timeout),
 * and then force-stops any remaining threads.
 */
@Component
@Slf4j
public class WorkerPool {

    private final JobService jobService;
    private final RetryService retryService;
    private final JobHandlerRegistry handlerRegistry;
    private final WorkerConfigProperties config;
    private final MeterRegistry meterRegistry;
    private volatile ExecutorService executor;
    private final String nodeId;

    /**
     * Volatile flag checked by every worker thread on each loop iteration.
     * Set to false during shutdown to signal workers to stop after their
     * current job finishes — they will NOT claim new batches.
     */
    private volatile boolean running = false;

    public WorkerPool(JobService jobService,
                      RetryService retryService,
                      JobHandlerRegistry handlerRegistry,
                      WorkerConfigProperties config,
                      MeterRegistry meterRegistry) {
        this.jobService = jobService;
        this.retryService = retryService;
        this.handlerRegistry = handlerRegistry;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.nodeId = generateNodeId();

        this.executor = buildExecutor(config.getThreads());
    }

    /**
     * Gracefully stop the current pool and start a fresh one with a
     * different thread count. Used by the load-test harness to benchmark
     * multiple thread configurations within a single JVM run.
     * <p>
     * Waits up to {@code shutdownTimeoutSeconds} for in-flight jobs to
     * complete before replacing the executor.
     *
     * @param newThreadCount the number of worker threads in the new pool
     */
    public synchronized void resize(int newThreadCount) throws InterruptedException {
        log.info("Resizing worker pool: stopping current pool, starting {} threads", newThreadCount);
        running = false;
        executor.shutdown();
        if (!executor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
        executor = buildExecutor(newThreadCount);
        running = true;
        for (int i = 0; i < newThreadCount; i++) {
            String workerId = nodeId + "-r" + i; // 'r' prefix marks resized workers in logs
            executor.submit(() -> workerLoop(workerId));
        }
        log.info("Worker pool resized to {} threads (node={})", newThreadCount, nodeId);
    }

    /** Creates a named-thread fixed thread pool. */
    private ExecutorService buildExecutor(int threadCount) {
        AtomicInteger counter = new AtomicInteger(0);
        return Executors.newFixedThreadPool(threadCount, runnable -> {
            Thread thread = new Thread(runnable, "jobqueue-worker-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Start worker threads AFTER stale job recovery has completed.
     * The @Order(2) ensures this runs after StaleJobRecovery's @Order(1).
     */
    @EventListener(ApplicationReadyEvent.class)
    @Order(2)
    public void start() {
        running = true;
        log.info("Starting {} worker threads (node={})", config.getThreads(), nodeId);

        for (int i = 0; i < config.getThreads(); i++) {
            String workerId = nodeId + "-" + i;
            executor.submit(() -> workerLoop(workerId));
        }

        log.info("Worker pool started. Registered handlers: {}", handlerRegistry.getRegisteredTypes());
    }

    /**
     * Graceful shutdown sequence:
     * <ol>
     *   <li>Set {@code running = false} → workers finish their current job
     *       but do NOT claim new batches.</li>
     *   <li>Call {@code executor.shutdown()} → no new tasks accepted.</li>
     *   <li>Await termination up to the configured timeout.</li>
     *   <li>If workers are still running after the timeout, call
     *       {@code shutdownNow()} to interrupt sleeping threads.</li>
     * </ol>
     * <p>
     * Any job that was mid-execution when the process was killed ungracefully
     * (kill -9) will be left in RUNNING status and recovered by
     * {@link StaleJobRecovery} on the next startup.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Initiating graceful shutdown of worker pool (node={})...", nodeId);
        running = false;
        executor.shutdown();

        try {
            if (!executor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.warn("Workers did not finish within {} seconds — forcing shutdown",
                         config.getShutdownTimeoutSeconds());
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown interrupted — forcing immediate shutdown");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Worker pool shut down (node={})", nodeId);
    }

    // ---------------------------------------------------------------
    //  Worker loop
    // ---------------------------------------------------------------

    /**
     * The main loop for each worker thread. Claims a batch of jobs,
     * processes each one, then either sleeps (if no jobs were found)
     * or immediately polls again (if jobs were found, more may be waiting).
     */
    private void workerLoop(String workerId) {
        log.info("Worker {} started", workerId);

        while (running) {
            try {
                List<Job> jobs = jobService.claimJobs(workerId, config.getBatchSize());

                if (jobs.isEmpty()) {
                    // No work available — back off to avoid hammering the DB
                    Thread.sleep(config.getPollIntervalMs());
                    continue;
                }

                for (Job job : jobs) {
                    if (!running) {
                        // Shutdown signal received — stop processing the batch.
                        // Remaining claimed jobs stay in RUNNING and will be
                        // recovered by StaleJobRecovery on the next startup.
                        log.info("Worker {} stopping mid-batch due to shutdown signal", workerId);
                        break;
                    }

                    // Record claimed counter
                    counter("jobqueue.jobs.claimed", job.getJobType()).increment();

                    // Record wait time: how long the job sat in the queue
                    recordWaitTime(job);

                    processJob(workerId, job);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Catch-all so a transient error (e.g., DB blip) doesn't kill
                // the worker thread. Sleep before retrying to avoid a tight
                // error loop.
                log.error("Worker {} encountered error during poll cycle", workerId, e);
                try {
                    Thread.sleep(config.getPollIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Worker {} stopped", workerId);
    }

    /**
     * Process a single claimed job: look up its handler, execute it,
     * and record the outcome with Micrometer metrics.
     */
    private void processJob(String workerId, Job job) {
        JobHandler handler = handlerRegistry.getHandler(job.getJobType());

        if (handler == null) {
            log.error("No handler registered for job type '{}' — routing job {} through retry",
                      job.getJobType(), job.getId());
            counter("jobqueue.jobs.failed", job.getJobType()).increment();
            retryService.handleFailure(job,
                    new IllegalStateException("No handler registered for job type: " + job.getJobType()));
            return;
        }

        // Time the handler execution
        Timer.Sample timerSample = Timer.start(meterRegistry);

        try {
            log.debug("Worker {} executing job {} (type={}, attempt={})",
                      workerId, job.getId(), job.getJobType(), job.getAttempts());

            handler.handle(job);

            // Record duration
            timerSample.stop(timer("jobqueue.job.duration", job.getJobType()));

            // Record success
            counter("jobqueue.jobs.succeeded", job.getJobType()).increment();

            // Record final retry count as a distribution summary
            retries("jobqueue.job.retries", job.getJobType())
                    .record(job.getAttempts());

            jobService.markSucceeded(job.getId());
            log.info("Worker {} completed job {} successfully", workerId, job.getId());

        } catch (Exception e) {
            // Record duration even on failure
            timerSample.stop(timer("jobqueue.job.duration", job.getJobType()));

            // Record failure
            counter("jobqueue.jobs.failed", job.getJobType()).increment();

            log.error("Worker {} — job {} failed: {}", workerId, job.getId(), e.getMessage(), e);
            // Hand off to retry service: increments attempts, computes
            // exponential backoff delay, and either reschedules as PENDING
            // or moves to DEAD_LETTER if attempts are exhausted.
            retryService.handleFailure(job, e);
        }
    }

    // ---------------------------------------------------------------
    //  Metrics helpers
    // ---------------------------------------------------------------

    /**
     * Record how long the job waited in the queue before being claimed.
     * Measured as (now - scheduled_at).
     */
    private void recordWaitTime(Job job) {
        if (job.getScheduledAt() != null) {
            Duration waitTime = Duration.between(job.getScheduledAt(), Instant.now());
            if (!waitTime.isNegative()) {
                timer("jobqueue.job.wait_time", job.getJobType())
                        .record(waitTime);
            }
        }
    }

    private Counter counter(String name, String jobType) {
        return Counter.builder(name)
                .tag("job_type", jobType)
                .register(meterRegistry);
    }

    private Timer timer(String name, String jobType) {
        return Timer.builder(name)
                .tag("job_type", jobType)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    private DistributionSummary retries(String name, String jobType) {
        return DistributionSummary.builder(name)
                .tag("job_type", jobType)
                .register(meterRegistry);
    }

    // ---------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------

    private String generateNodeId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
