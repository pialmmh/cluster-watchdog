package com.telcobright.watchdog.model;

import java.time.Instant;

public record ServiceStatus(
        String name,
        String type,
        Status status,
        String message,
        Instant checkedAt
) {
    public enum Status {
        UP, DOWN, DEGRADED, UNKNOWN
    }

    public static ServiceStatus up(String name, String type) {
        return new ServiceStatus(name, type, Status.UP, "Running", Instant.now());
    }

    public static ServiceStatus down(String name, String type, String message) {
        return new ServiceStatus(name, type, Status.DOWN, message, Instant.now());
    }

    public static ServiceStatus degraded(String name, String type, String message) {
        return new ServiceStatus(name, type, Status.DEGRADED, message, Instant.now());
    }

    public static ServiceStatus unknown(String name, String type, String message) {
        return new ServiceStatus(name, type, Status.UNKNOWN, message, Instant.now());
    }
}
