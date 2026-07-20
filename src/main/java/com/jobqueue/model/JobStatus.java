package com.jobqueue.model;

public enum JobStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    DEAD_LETTER
}
