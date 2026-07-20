package com.jobqueue.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "jobqueue.worker")
@Getter
@Setter
public class WorkerConfigProperties {

    /** Number of worker threads in the pool. */
    private int threads = 8;

    /** Milliseconds to sleep between poll cycles when no jobs are found. */
    private long pollIntervalMs = 1000;

    /** Maximum number of jobs to claim per poll cycle per thread. */
    private int batchSize = 5;

    /** Seconds to wait for in-flight jobs during graceful shutdown. */
    private int shutdownTimeoutSeconds = 30;

    /**
     * Minutes after which a RUNNING job with a stale locked_at timestamp
     * is considered abandoned and will be reset to PENDING on startup.
     */
    private int staleThresholdMinutes = 10;
}
