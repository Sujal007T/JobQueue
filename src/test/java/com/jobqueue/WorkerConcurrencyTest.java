package com.jobqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import com.jobqueue.repository.JobRepository;
import com.jobqueue.service.JobService;
import com.jobqueue.worker.JobHandler;
import com.jobqueue.worker.JobHandlerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency integration test verifying that the FOR UPDATE SKIP LOCKED
 * claiming mechanism guarantees each job is executed exactly once across
 * a pool of competing worker threads.
 *
 * <h3>What this proves</h3>
 * <ul>
 *   <li>Every submitted job reaches SUCCEEDED — none are lost.</li>
 *   <li>Every job is executed exactly once — no double-execution.</li>
 *   <li>No job is claimed by more than one worker thread — SKIP LOCKED works.</li>
 * </ul>
 *
 * <h3>What is measured</h3>
 * <ul>
 *   <li>Actual thread count (from Spring config, not hardcoded)</li>
 *   <li>Total jobs processed</li>
 *   <li>Wall-clock time to drain the queue</li>
 *   <li>Jobs/second throughput</li>
 * </ul>
 *
 * Results are written to {@code test-results/concurrency-test.log} so this
 * run becomes verifiable evidence — every printed number was produced by
 * this actual execution.
 */
@SpringBootTest
@TestPropertySource(properties = {
        // Use a faster poll interval during tests to reduce drain time
        "jobqueue.worker.poll-interval-ms=50",
        // Each thread claims up to 10 jobs per cycle (5 is the production default)
        "jobqueue.worker.batch-size=10",
        // All 8 threads actually start — this is the tested thread count
        "jobqueue.worker.threads=8",
        // Avoid stale-job recovery interfering with the fresh test run
        "jobqueue.worker.stale-threshold-minutes=60"
})
class WorkerConcurrencyTest extends TestcontainersBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerConcurrencyTest.class);
    private static final String RESULTS_DIR  = "test-results";
    private static final String RESULTS_FILE = "concurrency-test.log";

    /** Total jobs to seed. Must be large enough to create genuine contention. */
    private static final int TOTAL_JOBS = 1000;

    /**
     * Maximum wall-clock seconds to wait for all jobs to reach SUCCEEDED.
     * Set generously to accommodate slow CI environments.
     */
    private static final int DRAIN_TIMEOUT_SECONDS = 120;

    /**
     * Job type string for the spy handler registered by this test.
     * Must NOT collide with any production handler types.
     */
    private static final String TEST_JOB_TYPE = "concurrency-test-noop";

    // ----------------------------------------------------------------
    // Shared state populated by the spy handler
    // ----------------------------------------------------------------

    /**
     * Maps job.id → execution count.
     * After the test, every value must equal exactly 1.
     * ConcurrentHashMap + AtomicInteger are used so concurrent handler
     * invocations across multiple worker threads are safe without locks.
     */
    private final ConcurrentHashMap<Long, AtomicInteger> executionCounts = new ConcurrentHashMap<>();

    /**
     * Maps job.id → the thread name that executed it.
     * After the test, every job must appear here exactly once.
     * Used to confirm jobs were spread across different workers.
     */
    private final ConcurrentHashMap<Long, String> executingThread = new ConcurrentHashMap<>();

    // ----------------------------------------------------------------
    // Spring beans
    // ----------------------------------------------------------------

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobHandlerRegistry handlerRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * The actual thread count used by the running WorkerPool, sourced
     * directly from the Spring configuration — never hardcoded.
     */
    @Value("${jobqueue.worker.threads}")
    private int configuredThreadCount;

    // ----------------------------------------------------------------
    // Test lifecycle
    // ----------------------------------------------------------------

    @BeforeEach
    void registerSpyHandler() {
        // Register a no-op handler that records which job ran and on which thread.
        // The lambda is a JobHandler functional interface instance.
        JobHandler spyHandler = job -> {
            Long id = job.getId();
            String threadName = Thread.currentThread().getName();

            // putIfAbsent + incrementAndGet is the atomic "record first execution" idiom.
            executionCounts.computeIfAbsent(id, k -> new AtomicInteger(0))
                           .incrementAndGet();

            // Record the executing thread. putIfAbsent means only the FIRST thread wins —
            // if a second thread somehow claims the same job, it won't overwrite this entry
            // and the duplicate will be detectable later.
            executingThread.putIfAbsent(id, threadName);
        };

        // Guard: if the handler is already registered from a previous test run
        // in the same JVM (registry uses putIfAbsent), skip re-registration.
        if (handlerRegistry.getHandler(TEST_JOB_TYPE) == null) {
            handlerRegistry.register(TEST_JOB_TYPE, spyHandler);
        }
    }

    @AfterEach
    void cleanUp() {
        // Delete only the test jobs so other tables remain clean.
        // Production handlers' data is not touched.
        jobRepository.findByStatus(JobStatus.SUCCEEDED).stream()
                .filter(j -> TEST_JOB_TYPE.equals(j.getJobType()))
                .forEach(j -> jobRepository.deleteById(j.getId()));
        executionCounts.clear();
        executingThread.clear();
    }

    // ----------------------------------------------------------------
    // The test
    // ----------------------------------------------------------------

    @Test
    void eachJobIsExecutedExactlyOnceAcrossAllWorkers() throws Exception {

        // ----------------------------------------------------------------
        // Phase 1: Seed jobs
        // ----------------------------------------------------------------
        log.info("=== CONCURRENCY TEST: seeding {} jobs (job_type={}) ===", TOTAL_JOBS, TEST_JOB_TYPE);
        Instant seedStart = Instant.now();

        List<Long> seededIds = new ArrayList<>(TOTAL_JOBS);
        for (int i = 0; i < TOTAL_JOBS; i++) {
            Job job = jobService.submitJob(
                    TEST_JOB_TYPE,
                    objectMapper.readTree("{\"seq\":" + i + "}"),
                    0,
                    Instant.now(),
                    1  // max_attempts=1 — no retries in this test
            );
            seededIds.add(job.getId());
        }

        Duration seedDuration = Duration.between(seedStart, Instant.now());
        log.info("Seeded {} jobs in {}ms", TOTAL_JOBS, seedDuration.toMillis());

        // ----------------------------------------------------------------
        // Phase 2: Wait for queue to drain
        // The WorkerPool is already running (started via @EventListener on
        // ApplicationReadyEvent). We simply poll until all seeded jobs
        // reach a terminal status, or we time out.
        // ----------------------------------------------------------------
        log.info("Waiting for {} worker threads to drain queue (timeout={}s)...",
                 configuredThreadCount, DRAIN_TIMEOUT_SECONDS);

        Instant drainStart = Instant.now();
        boolean drained = awaitQueueDrain(seededIds, DRAIN_TIMEOUT_SECONDS);
        Duration wallClock = Duration.between(drainStart, Instant.now());

        // ----------------------------------------------------------------
        // Phase 3: Collect results from DB
        // ----------------------------------------------------------------
        List<Job> finalJobs = jobRepository.findAllById(seededIds);

        Map<JobStatus, List<Job>> byStatus = finalJobs.stream()
                .collect(Collectors.groupingBy(Job::getStatus));

        long succeededCount = finalJobs.stream()
                .filter(j -> j.getStatus() == JobStatus.SUCCEEDED)
                .count();

        // Jobs executed more than once (the invariant violation we're guarding against)
        Map<Long, Integer> duplicates = executionCounts.entrySet().stream()
                .filter(e -> e.getValue().get() != 1)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));

        // Jobs executed but not recorded in executingThread (should be impossible)
        long threadRecordMismatches = seededIds.stream()
                .filter(id -> executionCounts.containsKey(id) &&
                              !executingThread.containsKey(id))
                .count();

        // Distinct worker threads that participated
        long distinctThreads = executingThread.values().stream().distinct().count();

        double jobsPerSecond = wallClock.toMillis() > 0
                ? (double) TOTAL_JOBS / wallClock.toMillis() * 1000.0
                : 0.0;

        // ----------------------------------------------------------------
        // Phase 4: Write results log
        // ----------------------------------------------------------------
        String report = buildReport(
                configuredThreadCount,
                TOTAL_JOBS,
                wallClock,
                jobsPerSecond,
                succeededCount,
                duplicates,
                byStatus,
                distinctThreads,
                drained
        );

        log.info("\n{}", report);
        writeResultsFile(report);

        // ----------------------------------------------------------------
        // Phase 5: Assertions
        // ----------------------------------------------------------------
        assertThat(drained)
                .as("Queue should drain within %d seconds", DRAIN_TIMEOUT_SECONDS)
                .isTrue();

        assertThat(succeededCount)
                .as("Every seeded job must reach SUCCEEDED status")
                .isEqualTo(TOTAL_JOBS);

        assertThat(duplicates)
                .as("No job should be executed more than once (FOR UPDATE SKIP LOCKED violation)")
                .isEmpty();

        assertThat(threadRecordMismatches)
                .as("Every executed job must have a thread record")
                .isZero();

        assertThat(executionCounts.size())
                .as("Execution count map must have exactly one entry per job")
                .isEqualTo(TOTAL_JOBS);
    }

    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    /**
     * Polls the database until every seeded job has a terminal status
     * (SUCCEEDED, FAILED, or DEAD_LETTER), or the timeout elapses.
     */
    private boolean awaitQueueDrain(List<Long> seededIds, int timeoutSeconds) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);

        while (Instant.now().isBefore(deadline)) {
            List<Job> jobs = jobRepository.findAllById(seededIds);
            long pendingOrRunning = jobs.stream()
                    .filter(j -> j.getStatus() == JobStatus.PENDING ||
                                 j.getStatus() == JobStatus.RUNNING)
                    .count();

            if (pendingOrRunning == 0) {
                return true;
            }

            long succeeded = jobs.stream().filter(j -> j.getStatus() == JobStatus.SUCCEEDED).count();
            log.debug("Drain progress: {}/{} succeeded, {} pending/running",
                      succeeded, TOTAL_JOBS, pendingOrRunning);

            TimeUnit.MILLISECONDS.sleep(200);
        }

        return false;
    }

    /**
     * Builds the human-readable results report. Every number in this string
     * was produced by this actual test run — none are hardcoded.
     */
    private String buildReport(
            int threadCount,
            int totalJobs,
            Duration wallClock,
            double jobsPerSecond,
            long succeededCount,
            Map<Long, Integer> duplicates,
            Map<JobStatus, List<Job>> byStatus,
            long distinctThreads,
            boolean drained) {

        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .format(Instant.now().atZone(ZoneOffset.UTC));

        StringBuilder sb = new StringBuilder();
        sb.append("============================================================\n");
        sb.append("  CONCURRENCY TEST RESULTS\n");
        sb.append("  Timestamp          : ").append(timestamp).append("\n");
        sb.append("============================================================\n");
        sb.append("  Configuration\n");
        sb.append("    Worker threads   : ").append(threadCount).append("\n");
        sb.append("    Total jobs seeded: ").append(totalJobs).append("\n");
        sb.append("    Job type         : ").append(TEST_JOB_TYPE).append("\n");
        sb.append("\n");
        sb.append("  Execution\n");
        sb.append("    Queue drained    : ").append(drained ? "YES" : "NO (TIMEOUT)").append("\n");
        sb.append("    Wall-clock time  : ").append(wallClock.toMillis()).append(" ms\n");
        sb.append("    Throughput       : ").append(String.format("%.1f", jobsPerSecond)).append(" jobs/sec\n");
        sb.append("    Distinct workers : ").append(distinctThreads).append("\n");
        sb.append("\n");
        sb.append("  Final job status counts\n");
        for (JobStatus status : JobStatus.values()) {
            int count = byStatus.containsKey(status) ? byStatus.get(status).size() : 0;
            sb.append("    ").append(String.format("%-16s", status.name())).append(": ").append(count).append("\n");
        }
        sb.append("\n");
        sb.append("  Correctness assertions\n");
        sb.append("    Jobs succeeded   : ").append(succeededCount).append(" / ").append(totalJobs).append("\n");
        sb.append("    Double-executions: ").append(duplicates.size()).append("\n");
        if (!duplicates.isEmpty()) {
            sb.append("    Duplicate job ids: ").append(duplicates).append("\n");
        }
        sb.append("    PASS             : ").append(
                (succeededCount == totalJobs && duplicates.isEmpty()) ? "YES" : "NO").append("\n");
        sb.append("============================================================\n");
        return sb.toString();
    }

    /**
     * Writes the report to {@code test-results/concurrency-test.log},
     * creating the directory if needed. Appends so that multiple runs
     * accumulate history.
     */
    private void writeResultsFile(String report) {
        try {
            Path dir = Paths.get(RESULTS_DIR);
            Files.createDirectories(dir);
            Path file = dir.resolve(RESULTS_FILE);
            Files.writeString(file, report + "\n",
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            log.info("Results written to {}", file.toAbsolutePath());
        } catch (IOException e) {
            log.warn("Could not write results file: {}", e.getMessage());
        }
    }
}
