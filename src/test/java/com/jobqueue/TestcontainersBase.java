package com.jobqueue;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class that spins up a single shared PostgreSQL container for all
 * integration tests in this module. The container is started once per JVM
 * and reused across test classes via the Testcontainers @Container static
 * lifecycle combined with @ServiceConnection auto-wiring.
 */
@Testcontainers
public abstract class TestcontainersBase {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("jobqueue")
            .withUsername("user")
            .withPassword("password");
}
