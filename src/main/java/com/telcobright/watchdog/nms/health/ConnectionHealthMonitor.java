package com.telcobright.watchdog.nms.health;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lightweight connection health monitor for Kafka consumers, Redis subscribers, etc.
 */
public class ConnectionHealthMonitor {

    private final String name;
    private final String type;

    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong errorsCount = new AtomicLong(0);
    private final AtomicLong reconnectCount = new AtomicLong(0);

    private final AtomicReference<Instant> lastActivity = new AtomicReference<>(Instant.now());
    private final AtomicReference<Instant> lastError = new AtomicReference<>(null);
    private final AtomicReference<Instant> lastReconnect = new AtomicReference<>(null);
    private final Instant createdAt = Instant.now();

    private final AtomicReference<String> lastErrorMessage = new AtomicReference<>(null);
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.CONNECTING);

    public ConnectionHealthMonitor(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Record successful activity.
     */
    public void recordActivity() {
        messagesReceived.incrementAndGet();
        lastActivity.set(Instant.now());
        state.set(ConnectionState.CONNECTED);
    }

    /**
     * Record activity with count.
     */
    public void recordActivity(int count) {
        messagesReceived.addAndGet(count);
        lastActivity.set(Instant.now());
        state.set(ConnectionState.CONNECTED);
    }

    /**
     * Record reconnection attempt.
     */
    public void recordReconnect() {
        reconnectCount.incrementAndGet();
        lastReconnect.set(Instant.now());
        state.set(ConnectionState.RECONNECTING);
    }

    /**
     * Record successful connection.
     */
    public void recordConnected() {
        lastActivity.set(Instant.now());
        state.set(ConnectionState.CONNECTED);
    }

    /**
     * Record error.
     */
    public void recordError(String message) {
        errorsCount.incrementAndGet();
        lastError.set(Instant.now());
        lastErrorMessage.set(message);
        state.set(ConnectionState.ERROR);
    }

    /**
     * Record disconnection.
     */
    public void recordDisconnected() {
        state.set(ConnectionState.DISCONNECTED);
    }

    /**
     * Check if connection is stalled.
     */
    public boolean isStalled(Duration threshold) {
        Instant last = lastActivity.get();
        if (last == null) return true;
        return Duration.between(last, Instant.now()).compareTo(threshold) > 0;
    }

    /**
     * Get seconds since last activity.
     */
    public long getSecondsSinceLastActivity() {
        Instant last = lastActivity.get();
        if (last == null) return -1;
        return Duration.between(last, Instant.now()).toSeconds();
    }

    /**
     * Check if connection is healthy.
     */
    public boolean isHealthy(Duration stallThreshold) {
        return state.get() == ConnectionState.CONNECTED && !isStalled(stallThreshold);
    }

    public String getName() { return name; }
    public String getType() { return type; }
    public long getMessagesReceived() { return messagesReceived.get(); }
    public long getErrorsCount() { return errorsCount.get(); }
    public long getReconnectCount() { return reconnectCount.get(); }
    public Instant getLastActivity() { return lastActivity.get(); }
    public Instant getLastError() { return lastError.get(); }
    public Instant getLastReconnect() { return lastReconnect.get(); }
    public Instant getCreatedAt() { return createdAt; }
    public String getLastErrorMessage() { return lastErrorMessage.get(); }
    public ConnectionState getState() { return state.get(); }

    /**
     * Get all stats as a map.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("name", name);
        stats.put("type", type);
        stats.put("state", state.get().name());
        stats.put("messagesReceived", messagesReceived.get());
        stats.put("errorsCount", errorsCount.get());
        stats.put("reconnectCount", reconnectCount.get());
        stats.put("secondsSinceLastActivity", getSecondsSinceLastActivity());
        stats.put("lastActivity", lastActivity.get() != null ? lastActivity.get().toString() : null);
        stats.put("lastError", lastError.get() != null ? lastError.get().toString() : null);
        stats.put("lastErrorMessage", lastErrorMessage.get());
        stats.put("lastReconnect", lastReconnect.get() != null ? lastReconnect.get().toString() : null);
        stats.put("createdAt", createdAt.toString());
        stats.put("uptimeSeconds", Duration.between(createdAt, Instant.now()).toSeconds());
        return stats;
    }

    /**
     * Get minimal stats.
     */
    public Map<String, Object> getMinimalStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("name", name);
        stats.put("state", state.get().name());
        stats.put("messages", messagesReceived.get());
        stats.put("errors", errorsCount.get());
        stats.put("reconnects", reconnectCount.get());
        stats.put("idleSeconds", getSecondsSinceLastActivity());
        return stats;
    }

    public enum ConnectionState {
        CONNECTING,
        CONNECTED,
        RECONNECTING,
        DISCONNECTED,
        ERROR
    }
}
