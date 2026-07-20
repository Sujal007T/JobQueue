package com.jobqueue.worker.handlers;

import com.jobqueue.model.Job;
import com.jobqueue.worker.JobHandler;
import com.jobqueue.worker.JobHandlerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Example handler for "send-email" jobs.
 * Self-registers with the {@link JobHandlerRegistry} at construction time.
 */
@Component
@Slf4j
public class SendEmailHandler implements JobHandler {

    public SendEmailHandler(JobHandlerRegistry registry) {
        registry.register("send-email", this);
    }

    @Override
    public void handle(Job job) throws Exception {
        log.info("[send-email] Processing job {} — payload: {}", job.getId(), job.getPayload());

        // Simulate real work (e.g., calling an SMTP gateway)
        Thread.sleep(500);

        log.info("[send-email] Email sent successfully for job {}", job.getId());
    }
}
