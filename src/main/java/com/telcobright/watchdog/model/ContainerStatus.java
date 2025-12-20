package com.telcobright.watchdog.model;

import java.time.Instant;

public record ContainerStatus(
        String name,
        String id,
        ContainerType type,
        State state,
        String statusMessage,
        String image
) {
    public enum ContainerType {
        DOCKER, LXC
    }

    public enum State {
        RUNNING, STOPPED, PAUSED, RESTARTING, ERROR, UNKNOWN
    }

    public static ContainerStatus running(String name, String id, ContainerType type, String image) {
        return new ContainerStatus(name, id, type, State.RUNNING, "Running", image);
    }

    public static ContainerStatus stopped(String name, String id, ContainerType type, String image) {
        return new ContainerStatus(name, id, type, State.STOPPED, "Stopped", image);
    }

    public static ContainerStatus notFound(String name, ContainerType type) {
        return new ContainerStatus(name, "", type, State.UNKNOWN, "Not found", "");
    }

    public static ContainerStatus unknown(String name, ContainerType type) {
        return new ContainerStatus(name, "", type, State.UNKNOWN, "Unknown", "");
    }

    public static ContainerStatus error(String name, ContainerType type, String message) {
        return new ContainerStatus(name, "", type, State.ERROR, message, "");
    }
}
