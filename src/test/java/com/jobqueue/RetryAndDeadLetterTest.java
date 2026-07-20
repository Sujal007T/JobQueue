package com.jobqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import com.jobqueue.service.JobService;
import com.jobqueue.worker.JobHandlerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the retry and dead-letter mechanisms.
 *
 * <h3>Test 1 — Transient failure then success</h3>
 * A handler that fails on attempts 1-3 and succeeds on attempt 4.
 * Asserts all jobs reach SUCCEEDED, and that the {@code scheduled_at}
 * timestamps stored in the database after each retry respect the
 * exponential backoff formula (within jitter tolerance).
 *
 * <h3>Test 2 — Permanent failure → DEAD_LETTER</h3>
 * A handler that always throws with {@code max_attempts=2}.
 * Asserts the job ends in DEAD_LETTER, {@code attempts == max_attempts},
 * and {@code last_error} is populated.
 *
 * <h3>Configuration</h3>
 * Base delay is set to 200ms so backoff intervals stay short enough for
 * test execution while still being measurable and distinct from each other.
 */
@SpringBootTest
@TestPropertySource(properties = {
        // Short base delay so backoff intervals are measurable but fast
        "jobqueue.retry.default-base-delay-ms=200",
        // No per-type overrides for test job types
        "jobqueue.retry.max-delay-ms=60000",
        // Fast poll so retried jobs are picked up quickly
        "jobqueue.worker.poll-interval-ms=50",
        "jobqueue.worker.batch-size=10",
        "jobqueue.worker.threads=4",
        "jobqueue.worker.stale-threshold-minutes=60"
})
class RetryAndDeadLetterTest extends TestcontainersBase {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    private static final String RESULTS_DIR  = "test-results";
    private static final String RESULTS_FILE = "retry-test.log";

    // Job type strings — distinct from any production handler
    private static final String JOB_TYPE_TRANSIENT = "retry-test-transient";
    private static final String JOB_TYPE_PERMANENT  = "retry-test-permanent";

    // Attempt on which the transient handler succeeds (1-based)
    private static final int SUCCESS_ON_ATTEMPT = 4;
    // max_attempts for the transient test
    private static final int TRANSIENT_MAX_ATTEMPTS = 5;
    // max_attempts for the dead-letter test
    private static final int DLQ_MAX_ATTEMPTS = 2;

    private static final int DRAIN_TIMEOUT_SECONDS = 120;

    /**
     * Per-job invocation counter, used by both handlers.
     * Maps job.id → total number of times the handler was called.
     */
    private final ConcurrentHashMap<Long, AtomicInteger> invocationCount = new ConcurrentHashMap<>();

    /**
     * Timestamps recorded just before each handler invocation.
     * Maps job.id → list of Instants, one per call, in order.
     * Used to verify backoff delays by comparing consecutive entries.
     */
    private final ConcurrentHashMap<Long, List<Instant>> invocationTimestamps = new ConcurrentHashMap<>();

    @Autowired private JobService jobService;
    @Autowired private JobRepository jobRepository;
    @Autowired private JobHandlerRegistry handlerRegistry;
    @Autowired private ObjectMapper objectMapper;

    /** Injected directly from the test property source — never hardcoded. */
    @Value("${jobqueue.retry.default-base-delay-ms}")
    private long configuredBaseDelayMs;

    @BeforeEach
    void registerHandlers() {
        // ----------------------------------------------------------------
        // Transient handler: fail on invocations 1..(SUCCESS_ON_ATTEMPT-1),
        // succeed on invocation SUCCESS_ON_ATTEMPT.
        // ----------------------------------------------------------------
        if (handlerRegistry.getHandler(JOB_TYPE_TRANSIENT) == null) {
            handlerRegistry.register(JOB_TYPE_TRANSIENT, job -> {
                Long id = job.getId();
                invocationTimestamps
                        .computeIfAbsent(id, k -> new ArrayList<>())
                        .add(Instant.now());
                int callNumber = invocationCount
                        .computeIfAbsent(id, k -> new AtomicInteger(0))
                        .incrementAndGet();

                if (callNumber < SUCCESS_ON_ATTEMPT) {
                    throw new RuntimeException("Deliberate transient failure on call " + callNumber);
                }
                // callNumber == SUCCESS_ON_ATTEMPT → return normally → SUCCEEDED
            });
        }

        // ----------------------------------------------------------------
        // Permanent handler: always throws, regardless of attempt number.
        // ----------------------------------------------------------------
        if (handlerRegistry.getHandler(JOB_TYPE_PERMANENT) == null) {
            handlerRegistry.register(JOB_TYPE_PERMANENT, job -> {
                Long id = job.getId();
                invocationTimestamps
                        .computeIfAbsent(id, k -> new ArrayList<>())
                        .add(Instant.now());
                invocationCount
                        .computeIfAbsent(id, k -> new AtomicInteger(0))
                        .incrementAndGet();
                throw new RuntimeException("Permanent failure — this handler always throws");
            });
        }
    }

    @AfterEach
    void cleanUp() {
        // Delete only test jobs to avoid polluting other tests
        for (JobStatus status : JobStatus.values()) {
            jobRepository.findByStatus(status).stream()
                    .filter(j -> JOB_TYPE_TRANSIENT.equals(j.getJobType())
                              || JOB_TYPE_PERMANENT.equals(j.getJobType()))
                    .forEach(j -> jobRepository.deleteById(j.getId()));
        }
        invocationCount.clear();
        invocationTimestamps.clear();
    }

    // ================================================================
    // Test 1: Transient failures with eventual success
    // ================================================================

    @Test
    @DisplayName("Jobs that fail transiently (3x) eventually reach SUCCEEDED via exponential backoff")
    void transientFailuresThenSuccess() throws Exception {
        int jobCount = 10; // small batch — we're testing retry behavior, not concurrency

        // ----------------------------------------------------------------
        // Seed jobs with max_attempts=5 (succeeds on attempt 4)
        // ----------------------------------------------------------------
        List<Long> ids = seedJobs(JOB_TYPE_TRANSIENT, jobCount, TRANSIENT_MAX_ATTEMPTS);

        // ----------------------------------------------------------------
        // Wait for all jobs to reach SUCCEEDED
        // ----------------------------------------------------------------
        log.info("=== RETRY TEST 1: {} jobs, max_attempts={}, succeeds on attempt {} ===",
                 jobCount, TRANSIENT_MAX_ATTEMPTS, SUCCESS_ON_ATTEMPT);
        Instant drainStart = Instant.now();
        boolean drained = awaitAllStatus(ids, JobStatus.SUCCEEDED, DRAIN_TIMEOUT_SECONDS);
        Duration wallClock = Duration.between(drainStart, Instant.now());

        // ----------------------------------------------------------------
        // Collect final state from DB
        // ----------------------------------------------------------------
        List<Job> finalJobs = jobRepository.findAllById(ids);

        // ----------------------------------------------------------------
        // Verify backoff delays on a representative job
        // For each job, the scheduledAt timestamps are stored in the DB
        // after each failure. We read them from the invocation timestamps
        // we captured in the handler (before each execution).
        // ----------------------------------------------------------------
        List<BackoffObservation> backoffObservations = new ArrayList<>();

        for (Long id : ids) {
            List<Instant> timestamps = invocationTimestamps.get(id);
            if (timestamps != null && timestamps.size() >= 2) {
                for (int i = 1; i < timestamps.size(); i++) {
                    // Actual delay between consecutive invocations
                    long actualDelayMs = Duration.between(timestamps.get(i - 1), timestamps.get(i)).toMillis();

                    // Expected delay: base × 2^i (attempt number i, 1-based after first failure)
                    // At attempt i (0-indexed gap), the formula uses attempts=i
                    long expectedBase = configuredBaseDelayMs * (1L << i);
                    // Allow up to 20% jitter above the base, plus 500ms scheduling overhead
                    long tolerance = (long)(expectedBase * 0.20) + 500;

                    backoffObservations.add(new BackoffObservation(id, i, expectedBase, actualDelayMs, tolerance));
                }
            }
        }

        // ----------------------------------------------------------------
        // Build and write report — every number from actual execution
        // ----------------------------------------------------------------
        int observedRetries = invocationCount.values().stream()
                .mapToInt(AtomicInteger::get)
                .map(calls -> calls - 1) // retries = total calls - 1 (the final success)
                .sum();

        String report = buildTransientReport(
                jobCount,
                TRANSIENT_MAX_ATTEMPTS,
                SUCCESS_ON_ATTEMPT,
                configuredBaseDelayMs,
                wallClock,
                drained,
                finalJobs,
                observedRetries,
                backoffObservations
        );

        log.info("\n{}", report);
        writeResultsFile(report);

        // ----------------------------------------------------------------
        // Assertions
        // ----------------------------------------------------------------
        assertThat(drained)
                .as("All jobs should reach SUCCEEDED within %d seconds", DRAIN_TIMEOUT_SECONDS)
                .isTrue();

        assertThat(finalJobs)
                .as("All %d jobs must have status SUCCEEDED", jobCount)
                .allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED,
                          "status == SUCCEEDED");

        assertThat(finalJobs)
                .as("Each job must show %d attempts in the DB", SUCCESS_ON_ATTEMPT)
                .allMatch(j -> j.getAttempts() == SUCCESS_ON_ATTEMPT,
                          "attempts == " + SUCCESS_ON_ATTEMPT);

        // Verify backoff delays are within the expected range
        for (BackoffObservation obs : backoffObservations) {
            assertThat(obs.actualDelayMs)
                    .as("Job %d gap %d: actual delay %dms should be >= expected base %dms",
                        obs.jobId, obs.gapIndex, obs.actualDelayMs, obs.expectedBaseMs)
                    .isGreaterThanOrEqualTo(obs.expectedBaseMs);

            assertThat(obs.actualDelayMs)
                    .as("Job %d gap %d: actual delay %dms should be within tolerance (base=%dms + 20%% jitter + 500ms overhead)",
                        obs.jobId, obs.gapIndex, obs.actualDelayMs, obs.expectedBaseMs)
                    .isLessThan(obs.expectedBaseMs + obs.toleranceMs);
        }
    }

    // ================================================================
    // Test 2: Permanent failure → DEAD_LETTER
    // ================================================================

    @Test
    @DisplayName("Jobs that always fail are moved to DEAD_LETTER with correct attempt count and error message")
    void permanentFailureReachesDEADLETTER() throws Exception {
        int jobCount = 5;

        log.info("=== RETRY TEST 2: {} jobs, max_attempts={}, handler always fails ===",
                 jobCount, DLQ_MAX_ATTEMPTS);

        List<Long> ids = seedJobs(JOB_TYPE_PERMANENT, jobCount, DLQ_MAX_ATTEMPTS);

        // Wait for all jobs to reach DEAD_LETTER
        boolean drained = awaitAllStatus(ids, JobStatus.DEAD_LETTER, DRAIN_TIMEOUT_SECONDS);

        List<Job> finalJobs = jobRepository.findAllById(ids);

        // The expected total invocations per job equals max_attempts (each attempt fails)
        int expectedCallsPerJob = DLQ_MAX_ATTEMPTS;

        // Observed retries = total failed attempts - 1 (the first attempt is not a retry)
        int observedRetriesPerJob = invocationCount.values().stream()
                .mapToInt(AtomicInteger::get)
                .map(calls -> calls - 1)
                .sum() / jobCount;

        String report = buildDlqReport(
                jobCount,
                DLQ_MAX_ATTEMPTS,
                configuredBaseDelayMs,
                drained,
                finalJobs,
                expectedCallsPerJob,
                observedRetriesPerJob
        );

        log.info("\n{}", report);
        writeResultsFile(report);

        // ----------------------------------------------------------------
        // Assertions
        // ----------------------------------------------------------------
        assertThat(drained)
                .as("All jobs should reach DEAD_LETTER within %d seconds", DRAIN_TIMEOUT_SECONDS)
                .isTrue();

        assertThat(finalJobs)
                .as("All %d jobs must have status DEAD_LETTER", jobCount)
                .allMatch(j -> j.getStatus() == JobStatus.DEAD_LETTER,
                          "status == DEAD_LETTER");

        assertThat(finalJobs)
                .as("Each job's attempts must equal max_attempts (%d)", DLQ_MAX_ATTEMPTS)
                .allMatch(j -> j.getAttempts() == DLQ_MAX_ATTEMPTS,
                          "attempts == " + DLQ_MAX_ATTEMPTS);

        assertThat(finalJobs)
                .as("Every dead-lettered job must have last_error populated")
                .allMatch(j -> j.getLastError() != null && !j.getLastError().isBlank(),
                          "last_error is not blank");

        assertThat(finalJobs)
                .as("last_error must contain the actual exception message")
                .allMatch(j -> j.getLastError().contains("Permanent failure"),
                          "last_error contains 'Permanent failure'");

        // Confirm invocation count: each job must have been called exactly max_attempts times
        for (Long id : ids) {
            assertThat(invocationCount.get(id).get())
                    .as("Job %d must be attempted exactly %d times", id, DLQ_MAX_ATTEMPTS)
                    .isEqualTo(DLQ_MAX_ATTEMPTS);
        }
    }

    // ================================================================
    // Helpers
    // ================================================================

    private List<Long> seedJobs(String jobType, int count, int maxAttempts) throws Exception {
        List<Long> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Job job = jobService.submitJob(
                    jobType,
                    objectMapper.readTree("{\"seq\":" + i + "}"),
                    0,
                    Instant.now(),
                    maxAttempts
            );
            ids.add(job.getId());
        }
        log.info("Seeded {} jobs (type={}, max_attempts={})", count, jobType, maxAttempts);
        return ids;
    }

    /**
     * Polls every 200ms until all jobs have the target status, or timeout.
     */
    private boolean awaitAllStatus(List<Long> ids, JobStatus target, int timeoutSeconds)
            throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);
        while (Instant.now().isBefore(deadline)) {
            List<Job> jobs = jobRepository.findAllById(ids);
            long matched = jobs.stream().filter(j -> j.getStatus() == target).count();
            long other   = jobs.size() - matched;
            if (other == 0) return true;
            log.debug("Awaiting {}: {}/{} done, {} remaining", target, matched, ids.size(), other);
            TimeUnit.MILLISECONDS.sleep(200);
        }
        return false;
    }

    // ================================================================
    // Report builders — all numbers come from actual execution
    // ================================================================

    private String buildTransientReport(int jobCount, int maxAttempts, int successOnAttempt,
            long baseDelayMs, Duration wallClock, boolean drained, List<Job> finalJobs,
            int observedTotalRetries, List<BackoffObservation> backoffObs) {

        String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .format(Instant.now().atZone(ZoneOffset.UTC));
        StringBuilder sb = new StringBuilder();
        sb.append("============================================================\n");
        sb.append("  RETRY TEST 1 — TRANSIENT FAILURE WITH EVENTUAL SUCCESS\n");
        sb.append("  Timestamp              : ").append(ts).append("\n");
        sb.append("============================================================\n");
        sb.append("  Configuration\n");
        sb.append("    Jobs seeded          : ").append(jobCount).append("\n");
        sb.append("    Configured max_attempts: ").append(maxAttempts).append("\n");
        sb.append("    Handler succeeds on  : attempt ").append(successOnAttempt).append("\n");
        sb.append("    Base delay (ms)      : ").append(baseDelayMs).append("\n");
        sb.append("\n");
        sb.append("  Execution\n");
        sb.append("    All jobs drained     : ").append(drained ? "YES" : "NO (TIMEOUT)").append("\n");
        sb.append("    Wall-clock time      : ").append(wallClock.toMillis()).append(" ms\n");
        sb.append("    Observed retries     : ").append(observedTotalRetries)
          .append(" (").append(observedTotalRetries / jobCount).append(" per job)\n");
        sb.append("    Expected retries/job : ").append(successOnAttempt - 1).append("\n");
        sb.append("\n");
        sb.append("  DB final state\n");
        finalJobs.forEach(j -> sb.append(String.format("    Job %-5d status=%-12s attempts=%d%n",
                j.getId(), j.getStatus(), j.getAttempts())));
        sb.append("\n");
        sb.append("  Backoff delay observations\n");
        sb.append(String.format("    %-8s %-6s %-14s %-14s %-10s%n",
                "Job", "Gap", "Expected(ms)", "Actual(ms)", "Within?"));
        for (BackoffObservation obs : backoffObs) {
            boolean within = obs.actualDelayMs >= obs.expectedBaseMs
                          && obs.actualDelayMs < obs.expectedBaseMs + obs.toleranceMs;
            sb.append(String.format("    %-8d %-6d %-14d %-14d %-10s%n",
                    obs.jobId, obs.gapIndex, obs.expectedBaseMs, obs.actualDelayMs,
                    within ? "YES" : "NO"));
        }
        sb.append("\n");
        sb.append("  PASS: ").append(
                drained && finalJobs.stream().allMatch(j -> j.getStatus() == JobStatus.SUCCEEDED)
                ? "YES" : "NO").append("\n");
        sb.append("============================================================\n");
        return sb.toString();
    }

    private String buildDlqReport(int jobCount, int maxAttempts, long baseDelayMs,
            boolean drained, List<Job> finalJobs, int expectedCalls, int observedRetriesPerJob) {

        String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .format(Instant.now().atZone(ZoneOffset.UTC));
        StringBuilder sb = new StringBuilder();
        sb.append("============================================================\n");
        sb.append("  RETRY TEST 2 — PERMANENT FAILURE → DEAD_LETTER\n");
        sb.append("  Timestamp              : ").append(ts).append("\n");
        sb.append("============================================================\n");
        sb.append("  Configuration\n");
        sb.append("    Jobs seeded          : ").append(jobCount).append("\n");
        sb.append("    Configured max_attempts: ").append(maxAttempts).append("\n");
        sb.append("    Base delay (ms)      : ").append(baseDelayMs).append("\n");
        sb.append("\n");
        sb.append("  Execution\n");
        sb.append("    All jobs drained     : ").append(drained ? "YES" : "NO (TIMEOUT)").append("\n");
        sb.append("    Expected calls/job   : ").append(expectedCalls)
          .append(" (= max_attempts)").append("\n");
        sb.append("    Observed retries/job : ").append(observedRetriesPerJob)
          .append(" (= max_attempts - 1)").append("\n");
        sb.append("\n");
        sb.append("  DB final state\n");
        finalJobs.forEach(j -> sb.append(String.format(
                "    Job %-5d status=%-12s attempts=%d last_error='%s'%n",
                j.getId(), j.getStatus(), j.getAttempts(),
                j.getLastError() != null ? j.getLastError().substring(0,
                        Math.min(60, j.getLastError().length())) : "null")));
        sb.append("\n");
        sb.append("  PASS: ").append(
                drained && finalJobs.stream().allMatch(j ->
                        j.getStatus() == JobStatus.DEAD_LETTER &&
                        j.getAttempts() == maxAttempts &&
                        j.getLastError() != null)
                ? "YES" : "NO").append("\n");
        sb.append("============================================================\n");
        return sb.toString();
    }

    private void writeResultsFile(String report) {
        try {
            Path dir = Paths.get(RESULTS_DIR);
            Files.createDirectories(dir);
            Path file = dir.resolve(RESULTS_FILE);
            Files.writeString(file, report + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("Results written to {}", file.toAbsolutePath());
        } catch (IOException e) {
            log.warn("Could not write results file: {}", e.getMessage());
        }
    }

    // ================================================================
    // Value objects
    // ================================================================

    /** Captures a single delay observation for backoff verification. */
    private record BackoffObservation(
            long jobId,
            int gapIndex,        // 1-based gap number (1 = first retry gap)
            long expectedBaseMs, // base × 2^gapIndex (no jitter)
            long actualDelayMs,  // measured time between consecutive invocations
            long toleranceMs     // jitter ceiling + scheduling headroom
    ) {}
}
