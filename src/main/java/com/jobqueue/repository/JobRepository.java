package com.jobqueue.repository;

import com.jobqueue.model.Job;
import com.jobqueue.model.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<Job, Long>, JobRepositoryCustom {

    List<Job> findByStatus(JobStatus status);

    Page<Job> findByStatus(JobStatus status, Pageable pageable);

    long countByStatus(JobStatus status);

    /**
     * Reset RUNNING jobs with a stale locked_at back to PENDING.
     * Used by {@link com.jobqueue.worker.StaleJobRecovery} on startup
     * to recover jobs abandoned by ungraceful shutdowns.
     *
     * @param threshold jobs locked before this instant are considered stale
     * @return the number of jobs reset
     */
    @Modifying
    @Query(value = """
            UPDATE jobs
            SET status = 'PENDING',
                locked_by = NULL,
                locked_at = NULL,
                updated_at = NOW()
            WHERE status = 'RUNNING'
              AND locked_at < :threshold
            """, nativeQuery = true)
    int resetStaleRunningJobs(@Param("threshold") Instant threshold);
}
