package com.jobqueue.worker.handlers;

import com.jobqueue.model.Job;
import com.jobqueue.worker.JobHandler;
import com.jobqueue.worker.JobHandlerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Example handler for "process-payment" jobs.
 * Self-registers with the {@link JobHandlerRegistry} at construction time.
 */
@Component
@Slf4j
public class ProcessPaymentHandler implements JobHandler {

    public ProcessPaymentHandler(JobHandlerRegistry registry) {
        registry.register("process-payment", this);
    }

    @Override
    public void handle(Job job) throws Exception {
        log.info("[process-payment] Processing job {} — payload: {}", job.getId(), job.getPayload());

        // Simulate payment gateway call
        Thread.sleep(1000);

        log.info("[process-payment] Payment processed successfully for job {}", job.getId());
    }
}
