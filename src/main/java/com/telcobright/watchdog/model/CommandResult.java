package com.telcobright.watchdog.model;

import java.time.Duration;
import java.time.Instant;

public record CommandResult(
        String command,
        int exitCode,
        String stdout,
        String stderr,
        Duration duration,
        Instant executedAt
) {
    public boolean isSuccess() {
        return exitCode == 0;
    }

    public static CommandResult success(String command, String stdout, Duration duration) {
        return new CommandResult(command, 0, stdout, "", duration, Instant.now());
    }

    public static CommandResult failure(String command, int exitCode, String stdout, String stderr, Duration duration) {
        return new CommandResult(command, exitCode, stdout, stderr, duration, Instant.now());
    }

    public static CommandResult error(String command, String error) {
        return new CommandResult(command, -1, "", error, Duration.ZERO, Instant.now());
    }
}
