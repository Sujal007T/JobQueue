package com.jobqueue.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobqueue.model.Job;
import lombok.Data;

import java.time.Instant;

@Data
public class JobResponseDto {
    private Long id;
    private String jobType;
    private JsonNode payload;
    private String status;
    private Integer priority;
    private Instant scheduledAt;
    private Integer attempts;
    private Integer maxAttempts;
    private String lastError;
    private Instant createdAt;
    private Instant updatedAt;

    public static JobResponseDto fromEntity(Job job) {
        JobResponseDto dto = new JobResponseDto();
        dto.setId(job.getId());
        dto.setJobType(job.getJobType());
        dto.setPayload(job.getPayload());
        dto.setStatus(job.getStatus().name());
        dto.setPriority(job.getPriority());
        dto.setScheduledAt(job.getScheduledAt());
        dto.setAttempts(job.getAttempts());
        dto.setMaxAttempts(job.getMaxAttempts());
        dto.setLastError(job.getLastError());
        dto.setCreatedAt(job.getCreatedAt());
        dto.setUpdatedAt(job.getUpdatedAt());
        return dto;
    }
}
