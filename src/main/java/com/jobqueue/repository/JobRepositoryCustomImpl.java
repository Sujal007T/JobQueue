package com.jobqueue.repository;

import com.jobqueue.model.Job;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link JobRepositoryCustom} using {@link EntityManager}
 * with native SQL to leverage PostgreSQL's FOR UPDATE SKIP LOCKED.
 *
 * <h3>Why EntityManager with native queries instead of @Query?</h3>
 * <p>
 * Spring Data's @Query(nativeQuery = true) can technically run the SELECT,
 * but the claim operation is a multi-step read-then-write that must happen
 * atomically. Using EntityManager lets us orchestrate the SELECT and UPDATE
 * as two explicit steps within a single @Transactional method, making the
 * transactional boundary and the locking semantics visible in the code.
 * </p>
 *
 * <h3>Why FOR UPDATE SKIP LOCKED?</h3>
 * <p>
 * In a multi-worker job queue, multiple workers poll the same table
 * concurrently. Without coordination, two workers could SELECT the same
 * "best" row simultaneously, then both UPDATE it to RUNNING, causing
 * duplicate processing.
 * </p>
 * <p>
 * {@code FOR UPDATE} acquires a row-level exclusive lock on every row
 * returned by the SELECT. This prevents any other transaction from
 * modifying OR locking those rows until the current transaction commits.
 * </p>
 * <p>
 * {@code SKIP LOCKED} is the throughput enabler. Without it, a second
 * worker's SELECT would BLOCK waiting for the first worker's transaction
 * to release its locks — effectively serializing all workers into a
 * single-threaded queue. With SKIP LOCKED, PostgreSQL silently omits
 * any already-locked rows from the result set, so the second worker
 * instantly gets the NEXT batch of eligible jobs. Workers never wait
 * on each other.
 * </p>
 *
 * <h3>Why one atomic transaction?</h3>
 * <p>
 * The FOR UPDATE lock is held only for the lifetime of the owning
 * transaction. If we committed after the SELECT (releasing the locks)
 * and then opened a new transaction for the UPDATE, there would be a
 * window where another worker could lock and claim the same rows.
 * By wrapping SELECT + UPDATE in a single @Transactional method, the
 * rows stay locked from the moment they are selected until after the
 * UPDATE commits, making the entire claim operation atomic.
 * </p>
 */
public class JobRepositoryCustomImpl implements JobRepositoryCustom {

    @PersistenceContext
    private EntityManager em;

    @Override
    @Transactional
    @SuppressWarnings("unchecked")
    public List<Job> claimJobs(String workerId, int batchSize) {

        // ---------------------------------------------------------------
        // STEP 1: SELECT eligible jobs with FOR UPDATE SKIP LOCKED
        // ---------------------------------------------------------------
        // This query does three things simultaneously:
        //   1. Filters to only PENDING jobs whose scheduled_at has arrived.
        //   2. Orders by priority DESC (higher priority first), then by
        //      scheduled_at ASC (oldest first among equal priorities).
        //   3. Acquires an exclusive row-level lock (FOR UPDATE) on each
        //      returned row, with SKIP LOCKED to avoid blocking on rows
        //      already claimed by a concurrent worker.
        //
        // We SELECT only the IDs to minimize the data transferred under
        // lock and to keep the critical section as short as possible.
        // ---------------------------------------------------------------
        List<Long> claimedIds = em.createNativeQuery(
                """
                SELECT id FROM jobs
                WHERE status = 'PENDING'
                  AND scheduled_at <= NOW()
                ORDER BY priority DESC, scheduled_at ASC
                LIMIT :batchSize
                FOR UPDATE SKIP LOCKED
                """)
                .setParameter("batchSize", batchSize)
                .getResultList();

        if (claimedIds.isEmpty()) {
            return Collections.emptyList();
        }

        // ---------------------------------------------------------------
        // STEP 2: UPDATE the locked rows to RUNNING — same transaction
        // ---------------------------------------------------------------
        // This UPDATE runs while the FOR UPDATE locks from Step 1 are
        // still held (same transaction). No other worker can see or touch
        // these rows because:
        //   a) They are exclusively locked by our transaction.
        //   b) Any concurrent claimJobs() call will SKIP them.
        //
        // If this were a SEPARATE transaction, the sequence would be:
        //   T1: BEGIN → SELECT ... FOR UPDATE SKIP LOCKED → COMMIT (locks released!)
        //   T2: BEGIN → SELECT ... FOR UPDATE SKIP LOCKED → gets the SAME rows!
        //   T1: BEGIN → UPDATE ... SET status='RUNNING' → COMMIT
        //   T2: BEGIN → UPDATE ... SET status='RUNNING' → COMMIT (duplicate!)
        //
        // By doing it in ONE transaction:
        //   T1: BEGIN → SELECT (locks acquired) → UPDATE → COMMIT (locks released)
        //   T2: BEGIN → SELECT (sees locks, SKIPs those rows) → gets different rows
        // ---------------------------------------------------------------
        em.createNativeQuery(
                """
                UPDATE jobs
                SET status = 'RUNNING',
                    locked_by = :workerId,
                    locked_at = NOW(),
                    updated_at = NOW()
                WHERE id IN (:ids)
                """)
                .setParameter("workerId", workerId)
                .setParameter("ids", claimedIds)
                .executeUpdate();

        // ---------------------------------------------------------------
        // STEP 3: Return the claimed Job entities in their updated state
        // ---------------------------------------------------------------
        // We flush any pending writes and clear the persistence context
        // to ensure Hibernate reads the post-UPDATE state from the
        // database, not a stale first-level cache entry.
        // ---------------------------------------------------------------
        em.flush();
        em.clear();

        return em.createQuery(
                "SELECT j FROM Job j WHERE j.id IN :ids ORDER BY j.priority DESC, j.scheduledAt ASC",
                        Job.class)
                .setParameter("ids", claimedIds)
                .getResultList();
    }
}
