package com.jobqueue.worker;

import com.jobqueue.config.WorkerConfigProperties;
import com.jobqueue.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Startup recovery mechanism for jobs stranded in RUNNING status.
 * <p>
 * When the application is killed ungracefully (kill -9, OOM, power loss),
 * any job that was mid-execution will remain in RUNNING with a stale
 * {@code locked_at} timestamp. No worker will ever claim it again because
 * the polling query only selects PENDING jobs.
 * <p>
 * This component runs ONCE on startup, BEFORE the worker pool begins
 * polling (@Order(1) vs WorkerPool's @Order(2)). It finds all RUNNING
 * jobs whose {@code locked_at} is older than a configurable staleness
 * threshold and resets them to PENDING so they can be retried.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class StaleJobRecovery {

    private final JobService jobService;
    private final WorkerConfigProperties config;

    @EventListener(ApplicationReadyEvent.class)
    @Order(1) // Run BEFORE WorkerPool starts polling
    public void recoverStaleJobs() {
        Instant threshold = Instant.now().minus(config.getStaleThresholdMinutes(), ChronoUnit.MINUTES);
        int resetCount = jobService.resetStaleJobs(threshold);

        if (resetCount > 0) {
            log.warn("Startup recovery: reset {} stale RUNNING jobs back to PENDING " +
                     "(locked_at older than {} minutes)", resetCount, config.getStaleThresholdMinutes());
        } else {
            log.info("Startup recovery: no stale jobs found");
        }
    }
}
