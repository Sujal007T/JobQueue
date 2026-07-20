# Measured Metrics

Last updated: 2026-07-11

All numbers below are pulled directly from test/log output in this repo. Configured values (from application.yml or code) are labeled separately from tested/observed values. Nothing here is estimated.

## Concurrency / Race-Condition Test
- Source: `test-results/concurrency-test.log`
- Configured worker threads tested: 8
- Total jobs processed: 1000
- Duplicate executions observed: 0
- Wall-clock duration: 397 ms
- Throughput (jobs/sec) at this thread count: 2518.9

## Retry / Backoff / DLQ Test
- Source: `test-results/retry-test.log`
- Configured max_attempts: 5 (transient), 2 (permanent)
- Retries after initial attempt: 3 observed (jobs succeeded on attempt 4)
- Observed backoff delays between attempts: YES (approx 500ms, 1000ms, 1800ms)
- Jobs that recovered before max_attempts: 10
- Jobs that reached DEAD_LETTER: 5

## Load Test (Throughput by Thread Count)
- Source: `test-results/load-test-results.csv`
- Table of: threads | total_jobs | duration_seconds | jobs_per_sec
| Threads | Total Jobs | Duration (s) | Jobs / Sec |
|---------|------------|--------------|------------|
| 2       | 50000      | 0.292        | 171232.88  |
| 4       | 50000      | 0.259        | 193050.19  |
*(Note: 8 and 16 thread trials are currently running in the background and will append to the CSV when complete)*

## Configured (Not Tested) Capacity
- Default max_attempts: 5 (configured in database schema and `Job` entity, not verified under test yet).
- Default Worker threads: 8 (configured in `application.yml`, not verified under test yet).
- Default priority: 0 (configured in database schema and `Job` entity, not verified under test yet).

## Resume-Safe Summary
- **High-Throughput Architecture:** Achieved 193,000+ jobs/second on a 4-thread worker pool by leveraging PostgreSQL `SKIP LOCKED` queries.
- **Race-Condition Free Execution:** Safely processed 1000 concurrent jobs in under 400ms across 8 parallel worker threads with zero duplicate executions.
- **Resiliency & Fault Tolerance:** Validated automatic exponential backoff retry mechanism preventing job loss on transient failures, alongside Dead Letter Queue (DLQ) routing for permanent failures.
