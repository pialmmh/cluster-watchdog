package com.telcobright.watchdog.model;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record NetworkStatus(
        String interfaceName,
        InterfaceType type,
        State status,
        List<String> ipAddresses,
        Optional<String> lastHandshake,
        String message,
        Instant checkedAt
) {
    public enum InterfaceType {
        BRIDGE, WIREGUARD, PHYSICAL
    }

    public enum State {
        UP, DOWN, DEGRADED, UNKNOWN
    }

    public static NetworkStatus up(String name, InterfaceType type, List<String> ips) {
        return new NetworkStatus(name, type, State.UP, ips, Optional.empty(), "Up", Instant.now());
    }

    public static NetworkStatus upWithHandshake(String name, InterfaceType type, List<String> ips, String handshake) {
        return new NetworkStatus(name, type, State.UP, ips, Optional.of(handshake), "Up", Instant.now());
    }

    public static NetworkStatus down(String name, InterfaceType type, String message) {
        return new NetworkStatus(name, type, State.DOWN, List.of(), Optional.empty(), message, Instant.now());
    }

    public static NetworkStatus degraded(String name, InterfaceType type, List<String> ips, String message) {
        return new NetworkStatus(name, type, State.DEGRADED, ips, Optional.empty(), message, Instant.now());
    }
}
