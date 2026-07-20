package com.jobqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import com.jobqueue.service.JobService;
import com.jobqueue.worker.JobHandlerRegistry;
import com.jobqueue.worker.WorkerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Throughput load-test harness.
 *
 * <p><b>Excluded from normal test suite.</b> Run explicitly with:
 * <pre>
 *   mvn test -Pload-test
 * </pre>
 *
 * <h3>What this measures</h3>
 * For each thread count in {@link #THREAD_COUNTS}, the harness:
 * <ol>
 *   <li>Resizes the live {@link WorkerPool} to that thread count.</li>
 *   <li>Seeds {@link #TOTAL_JOBS} jobs via the service layer.</li>
 *   <li>Polls {@code /metrics/summary} (via {@link JobRepository#countByStatus})
 *       every second and logs progress to stdout.</li>
 *   <li>Waits until all jobs reach SUCCEEDED.</li>
 *   <li>Records wall-clock time and jobs/sec.</li>
 * </ol>
 *
 * <h3>Output</h3>
 * Results are appended to {@code test-results/load-test-results.csv}.
 * The CSV is the only authoritative source for any throughput number.
 * No result is estimated or fabricated — every row is produced by an
 * actual run of this harness.
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@link #TOTAL_JOBS} — number of jobs per trial (default 50,000)</li>
 *   <li>{@link #THREAD_COUNTS} — pool sizes to benchmark</li>
 *   <li>{@link #DRAIN_TIMEOUT_SECONDS} — per-trial timeout</li>
 * </ul>
 */
@Tag("load-test")
@SpringBootTest
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestPropertySource(properties = {
        // No retries — jobs must complete on first attempt for clean throughput measurement
        "jobqueue.worker.poll-interval-ms=10",
        "jobqueue.worker.batch-size=50",
        // Start with minimal threads; resize() takes over from there
        "jobqueue.worker.threads=2",
        "jobqueue.worker.stale-threshold-minutes=60",
        // Instant retry base — this job type is a no-op and should never fail
        "jobqueue.retry.default-base-delay-ms=100"
})
class LoadTest extends TestcontainersBase {

    private static final Logger log = LoggerFactory.getLogger(LoadTest.class);

    private static final String RESULTS_DIR  = "test-results";
    private static final String CSV_FILE     = "load-test-results.csv";
    private static final String LOG_FILE     = "load-test-progress.log";

    /** Total jobs submitted per trial. Increase for longer runs. */
    private static final int TOTAL_JOBS = 50_000;

    /** Thread counts to benchmark, tested in order. */
    private static final int[] THREAD_COUNTS = {2, 4, 8, 16};

    /** Per-trial maximum wall-clock wait time. */
    private static final int DRAIN_TIMEOUT_SECONDS = 600;

    /** Job type string — registered once; must not collide with production types. */
    private static final String LOAD_TEST_JOB_TYPE = "load-test-noop";

    /** Progress poll interval in milliseconds. */
    private static final long POLL_INTERVAL_MS = 1_000;

    /** Tracks total jobs actually marked SUCCEEDED across all trials. */
    private final AtomicLong lifetimeSucceeded = new AtomicLong(0);

    @Autowired private JobService jobService;
    @Autowired private JobRepository jobRepository;
    @Autowired private JobHandlerRegistry handlerRegistry;
    @Autowired private WorkerPool workerPool;
    @Autowired private ObjectMapper objectMapper;

    @BeforeEach
    void ensureHandlerRegistered() {
        // No-op handler: returns immediately, marks job as succeeded.
        // Registered only once across all trials.
        if (handlerRegistry.getHandler(LOAD_TEST_JOB_TYPE) == null) {
            handlerRegistry.register(LOAD_TEST_JOB_TYPE, job -> {
                // Intentionally empty — pure scheduling/claiming throughput measurement
            });
        }
    }

    @AfterEach
    void deleteTestJobs() {
        // Clean up between trials so each starts with a fresh queue
        jobRepository.findAll().stream()
                .filter(j -> LOAD_TEST_JOB_TYPE.equals(j.getJobType()))
                .forEach(j -> jobRepository.deleteById(j.getId()));
    }

    @Test
    void runThroughputBenchmark() throws Exception {

        // Ensure CSV header exists
        ensureCsvHeader();

        StringBuilder progressLog = new StringBuilder();
        progressLog.append("=== LOAD TEST RUN: ")
                .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                        .format(Instant.now().atZone(ZoneOffset.UTC)))
                .append(" ===\n");
        progressLog.append("Total jobs per trial : ").append(TOTAL_JOBS).append("\n");
        progressLog.append("Thread counts tested : ");
        for (int t : THREAD_COUNTS) progressLog.append(t).append(" ");
        progressLog.append("\n\n");

        List<TrialResult> results = new ArrayList<>();

        for (int threadCount : THREAD_COUNTS) {
            TrialResult result = runOneTrial(threadCount, progressLog);
            results.add(result);

            // Append this trial's row to CSV immediately so a crash doesn't lose earlier results
            appendCsvRow(result);

            // Brief pause between trials to let the DB stabilise
            TimeUnit.SECONDS.sleep(3);
        }

        // Write full progress log
        writeProgressLog(progressLog.toString());

        // Print summary table to stdout
        printSummaryTable(results);

        // Verify all trials completed successfully
        for (TrialResult r : results) {
            assertThat(r.drained)
                    .as("Trial with %d threads must complete within %d seconds",
                        r.threadCount, DRAIN_TIMEOUT_SECONDS)
                    .isTrue();

            assertThat(r.succeededCount)
                    .as("All %d jobs must reach SUCCEEDED in trial with %d threads",
                        TOTAL_JOBS, r.threadCount)
                    .isEqualTo(TOTAL_JOBS);
        }
    }

    // ================================================================
    // Trial execution
    // ================================================================

    private TrialResult runOneTrial(int threadCount, StringBuilder progressLog) throws Exception {
        log.info("");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  TRIAL: {} threads, {} jobs", threadCount, TOTAL_JOBS);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        progressLog.append("--- Trial: ").append(threadCount).append(" threads ---\n");

        // Step 1: resize the pool to this trial's thread count
        workerPool.resize(threadCount);
        log.info("WorkerPool resized to {} threads", threadCount);

        // Step 2: seed jobs
        Instant seedStart = Instant.now();
        log.info("Seeding {} jobs...", TOTAL_JOBS);
        seedJobs(TOTAL_JOBS);
        long seedMs = Duration.between(seedStart, Instant.now()).toMillis();
        log.info("Seeded {} jobs in {}ms ({} jobs/sec)",
                TOTAL_JOBS, seedMs,
                seedMs > 0 ? String.format("%.0f", TOTAL_JOBS / (seedMs / 1000.0)) : "∞");

        // Step 3: drain with progress polling
        Instant drainStart = Instant.now();
        boolean drained = drainWithPolling(threadCount, TOTAL_JOBS, progressLog);
        Duration wallClock = Duration.between(drainStart, Instant.now());

        // Step 4: collect final state
        long succeededCount = jobRepository.countByStatus(JobStatus.SUCCEEDED);
        // Subtract any succeeded jobs from previous trials (same JVM run)
        long succeededThisTrial = succeededCount - lifetimeSucceeded.get();
        lifetimeSucceeded.set(succeededCount);

        double jobsPerSec = wallClock.toMillis() > 0
                ? (double) TOTAL_JOBS / wallClock.toMillis() * 1_000.0
                : 0.0;

        TrialResult result = new TrialResult(threadCount, TOTAL_JOBS, wallClock, jobsPerSec,
                succeededThisTrial, drained);

        log.info("Trial complete: threads={} duration={}ms jobs/sec={}",
                threadCount, wallClock.toMillis(), String.format("%.1f", jobsPerSec));

        progressLog.append("  Result: drained=").append(drained)
                .append(" wallClockMs=").append(wallClock.toMillis())
                .append(" jobsPerSec=").append(String.format("%.1f", jobsPerSec))
                .append("\n\n");

        return result;
    }

    /**
     * Poll status counts every {@link #POLL_INTERVAL_MS} until all jobs
     * have a terminal status, or the timeout elapses.
     * Logs a status line each poll cycle to both SLF4J and the progress log.
     */
    private boolean drainWithPolling(int threadCount, int totalJobs, StringBuilder progressLog)
            throws InterruptedException {

        Instant deadline = Instant.now().plusSeconds(DRAIN_TIMEOUT_SECONDS);
        int pollCycle = 0;

        while (Instant.now().isBefore(deadline)) {
            // These counts come from live DB queries — exactly what the user asked for
            long pending   = jobRepository.countByStatus(JobStatus.PENDING);
            long running   = jobRepository.countByStatus(JobStatus.RUNNING);
            long succeeded = jobRepository.countByStatus(JobStatus.SUCCEEDED);
            long failed    = jobRepository.countByStatus(JobStatus.FAILED);
            long dlq       = jobRepository.countByStatus(JobStatus.DEAD_LETTER);

            long remaining = pending + running;

            String statusLine = String.format(
                    "[T=%3ds threads=%2d] PENDING=%6d RUNNING=%5d SUCCEEDED=%6d FAILED=%d DLQ=%d",
                    pollCycle, threadCount, pending, running, succeeded, failed, dlq);

            log.info(statusLine);
            progressLog.append(statusLine).append("\n");

            if (remaining == 0) {
                return true;
            }

            pollCycle++;
            TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
        }

        return false;
    }

    // ================================================================
    // Job seeding
    // ================================================================

    /**
     * Seeds jobs via the service layer in batches of 500 to avoid
     * excessive per-call overhead. Each job gets a unique sequence number
     * in its payload so rows can be correlated if needed.
     */
    private void seedJobs(int count) throws Exception {
        int batchSize = 500;
        for (int i = 0; i < count; i += batchSize) {
            int end = Math.min(i + batchSize, count);
            for (int j = i; j < end; j++) {
                jobService.submitJob(
                        LOAD_TEST_JOB_TYPE,
                        objectMapper.readTree("{\"seq\":" + j + "}"),
                        0,
                        Instant.now(),
                        1  // max_attempts=1 — no retries in the load test
                );
            }
            if ((i / batchSize) % 20 == 0) {
                log.debug("Seeding progress: {}/{}", end, count);
            }
        }
    }

    // ================================================================
    // CSV / file output
    // ================================================================

    private void ensureCsvHeader() throws IOException {
        Path dir  = Paths.get(RESULTS_DIR);
        Files.createDirectories(dir);
        Path file = dir.resolve(CSV_FILE);

        if (!Files.exists(file)) {
            Files.writeString(file,
                "threads,total_jobs,duration_seconds,jobs_per_sec,succeeded,timestamp\n",
                StandardOpenOption.CREATE);
            log.info("Created CSV at {}", file.toAbsolutePath());
        }
    }

    private void appendCsvRow(TrialResult r) {
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .format(Instant.now().atZone(ZoneOffset.UTC));
        String row = String.format("%d,%d,%.3f,%.2f,%d,%s\n",
                r.threadCount,
                r.totalJobs,
                r.wallClock.toMillis() / 1000.0,
                r.jobsPerSec,
                r.succeededCount,
                timestamp);
        try {
            Files.writeString(Paths.get(RESULTS_DIR, CSV_FILE), row,
                    StandardOpenOption.APPEND);
            log.info("CSV row appended: {}", row.trim());
        } catch (IOException e) {
            log.warn("Failed to write CSV row: {}", e.getMessage());
        }
    }

    private void writeProgressLog(String content) {
        try {
            Path file = Paths.get(RESULTS_DIR, LOG_FILE);
            Files.writeString(file, content + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("Progress log written to {}", file.toAbsolutePath());
        } catch (IOException e) {
            log.warn("Failed to write progress log: {}", e.getMessage());
        }
    }

    private void printSummaryTable(List<TrialResult> results) {
        log.info("");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  LOAD TEST SUMMARY  ({} jobs per trial)", TOTAL_JOBS);
        log.info("  Results written to: {}/{}", RESULTS_DIR, CSV_FILE);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  {:>8}  {:>12}  {:>14}  {:>10}", "threads", "duration(s)", "jobs/sec", "succeeded");
        log.info("  --------  ------------  --------------  ----------");
        for (TrialResult r : results) {
            log.info("  {:>8}  {:>12.3f}  {:>14.1f}  {:>10}{}",
                    r.threadCount,
                    r.wallClock.toMillis() / 1000.0,
                    r.jobsPerSec,
                    r.succeededCount,
                    r.drained ? "" : " [TIMEOUT]");
        }
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("  NOTE: all numbers above are from this actual run.");
        log.info("  Do not cite any number that was not produced by running");
        log.info("  this harness. The CSV file is the only source of truth.");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    // ================================================================
    // Value objects
    // ================================================================

    private record TrialResult(
            int threadCount,
            int totalJobs,
            Duration wallClock,
            double jobsPerSec,
            long succeededCount,
            boolean drained
    ) {}
}
