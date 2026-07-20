package com.jobqueue;

import com.jobqueue.config.RetryConfigProperties;
import com.jobqueue.config.WorkerConfigProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({WorkerConfigProperties.class, RetryConfigProperties.class})
public class JobqueueApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobqueueApplication.class, args);
    }
}
