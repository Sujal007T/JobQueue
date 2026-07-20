package com.jobqueue.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.time.Instant;

@Data
public class JobRequestDto {
    private String jobType;
    private JsonNode payload;
    private Integer priority;
    private Instant scheduledAt;
    private Integer maxAttempts;
}
