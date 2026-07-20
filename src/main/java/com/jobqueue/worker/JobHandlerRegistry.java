package com.jobqueue.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry mapping job type strings to their {@link JobHandler}
 * implementations. Handlers self-register at startup via their constructors.
 */
@Component
@Slf4j
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    /**
     * Register a handler for a given job type.
     *
     * @param jobType the job type string (e.g. "send-email")
     * @param handler the handler implementation
     * @throws IllegalStateException if a handler is already registered for this type
     */
    public void register(String jobType, JobHandler handler) {
        JobHandler existing = handlers.putIfAbsent(jobType, handler);
        if (existing != null) {
            throw new IllegalStateException(
                    "Duplicate handler registration for job type '" + jobType + "': " +
                    existing.getClass().getSimpleName() + " vs " + handler.getClass().getSimpleName());
        }
        log.info("Registered handler for job type '{}': {}", jobType, handler.getClass().getSimpleName());
    }

    /**
     * Look up the handler for a job type.
     *
     * @return the handler, or null if none is registered
     */
    public JobHandler getHandler(String jobType) {
        return handlers.get(jobType);
    }

    /**
     * @return an unmodifiable view of all registered job type names
     */
    public Set<String> getRegisteredTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }
}
