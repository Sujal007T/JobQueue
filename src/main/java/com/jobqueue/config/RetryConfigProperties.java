package com.jobqueue.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-job-type retry configuration bound to {@code jobqueue.retry.*}.
 *
 * <p>Example application.yml:</p>
 * <pre>
 * jobqueue:
 *   retry:
 *     default-base-delay-ms: 2000
 *     max-delay-ms: 3600000
 *     policies:
 *       send-email: 5000
 *       process-payment: 10000
 * </pre>
 */
@ConfigurationProperties(prefix = "jobqueue.retry")
@Getter
@Setter
public class RetryConfigProperties {

    /** Default base delay in milliseconds for exponential backoff. */
    private long defaultBaseDelayMs = 2000;

    /**
     * Hard upper bound on computed delay to prevent absurd wait times
     * if max_attempts is set very high. Default: 1 hour.
     */
    private long maxDelayMs = 3_600_000;

    /**
     * Per-job-type base delay overrides, keyed by job_type string.
     * Values are in milliseconds.
     * Job types not listed here fall back to {@link #defaultBaseDelayMs}.
     */
    private Map<String, Long> policies = new HashMap<>();

    /**
     * Returns the base delay for a given job type, falling back to the
     * default if no per-type override is configured.
     */
    public long getBaseDelayMs(String jobType) {
        return policies.getOrDefault(jobType, defaultBaseDelayMs);
    }
}
