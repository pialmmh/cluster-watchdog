package com.telcobright.watchdog.nms.health;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Central registry for all connection health monitors.
 */
@ApplicationScoped
public class ConnectionHealthRegistry {

    private static final Logger LOG = Logger.getLogger(ConnectionHealthRegistry.class);

    private final Map<String, ConnectionHealthMonitor> monitors = new ConcurrentHashMap<>();
    private static final Duration DEFAULT_STALL_THRESHOLD = Duration.ofMinutes(5);

    /**
     * Register a new connection monitor.
     */
    public ConnectionHealthMonitor register(String name, String type) {
        ConnectionHealthMonitor monitor = new ConnectionHealthMonitor(name, type);
        ConnectionHealthMonitor existing = monitors.putIfAbsent(name, monitor);
        if (existing != null) {
            LOG.warnf("Monitor '%s' already registered, returning existing", name);
            return existing;
        }
        LOG.infof("Registered connection monitor: %s (type: %s)", name, type);
        return monitor;
    }

    /**
     * Get monitor by name.
     */
    public ConnectionHealthMonitor get(String name) {
        return monitors.get(name);
    }

    /**
     * Get or create monitor.
     */
    public ConnectionHealthMonitor getOrCreate(String name, String type) {
        return monitors.computeIfAbsent(name, k -> {
            LOG.infof("Registered connection monitor: %s (type: %s)", name, type);
            return new ConnectionHealthMonitor(name, type);
        });
    }

    /**
     * Unregister a monitor.
     */
    public void unregister(String name) {
        ConnectionHealthMonitor removed = monitors.remove(name);
        if (removed != null) {
            LOG.infof("Unregistered connection monitor: %s", name);
        }
    }

    /**
     * Get all registered monitors.
     */
    public List<ConnectionHealthMonitor> getAllMonitors() {
        return new ArrayList<>(monitors.values());
    }

    /**
     * Get monitors by type.
     */
    public List<ConnectionHealthMonitor> getMonitorsByType(String type) {
        return monitors.values().stream()
            .filter(m -> type.equalsIgnoreCase(m.getType()))
            .collect(Collectors.toList());
    }

    /**
     * Get unhealthy connections.
     */
    public List<ConnectionHealthMonitor> getUnhealthyConnections(Duration stallThreshold) {
        return monitors.values().stream()
            .filter(m -> !m.isHealthy(stallThreshold))
            .collect(Collectors.toList());
    }

    /**
     * Get unhealthy connections with default threshold.
     */
    public List<ConnectionHealthMonitor> getUnhealthyConnections() {
        return getUnhealthyConnections(DEFAULT_STALL_THRESHOLD);
    }

    /**
     * Get stats for all connections.
     */
    public Map<String, Object> getAllStats() {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalConnections", monitors.size());
        summary.put("healthyCount", monitors.values().stream()
            .filter(m -> m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());
        summary.put("unhealthyCount", monitors.values().stream()
            .filter(m -> !m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());
        summary.put("totalMessages", monitors.values().stream()
            .mapToLong(ConnectionHealthMonitor::getMessagesReceived).sum());
        summary.put("totalErrors", monitors.values().stream()
            .mapToLong(ConnectionHealthMonitor::getErrorsCount).sum());
        summary.put("totalReconnects", monitors.values().stream()
            .mapToLong(ConnectionHealthMonitor::getReconnectCount).sum());
        result.put("summary", summary);

        List<Map<String, Object>> connections = monitors.values().stream()
            .map(ConnectionHealthMonitor::getStats)
            .collect(Collectors.toList());
        result.put("connections", connections);

        return result;
    }

    /**
     * Get minimal stats for all connections.
     */
    public Map<String, Object> getMinimalStats() {
        Map<String, Object> result = new HashMap<>();

        result.put("total", monitors.size());
        result.put("healthy", monitors.values().stream()
            .filter(m -> m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());
        result.put("unhealthy", monitors.values().stream()
            .filter(m -> !m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());

        List<Map<String, Object>> connections = monitors.values().stream()
            .map(ConnectionHealthMonitor::getMinimalStats)
            .collect(Collectors.toList());
        result.put("connections", connections);

        return result;
    }

    /**
     * Get stats grouped by type.
     */
    public Map<String, Object> getStatsByType() {
        Map<String, Object> result = new HashMap<>();

        Map<String, List<ConnectionHealthMonitor>> byType = monitors.values().stream()
            .collect(Collectors.groupingBy(ConnectionHealthMonitor::getType));

        for (Map.Entry<String, List<ConnectionHealthMonitor>> entry : byType.entrySet()) {
            String type = entry.getKey();
            List<ConnectionHealthMonitor> typeMonitors = entry.getValue();

            Map<String, Object> typeStats = new HashMap<>();
            typeStats.put("count", typeMonitors.size());
            typeStats.put("healthy", typeMonitors.stream()
                .filter(m -> m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());
            typeStats.put("totalMessages", typeMonitors.stream()
                .mapToLong(ConnectionHealthMonitor::getMessagesReceived).sum());
            typeStats.put("totalErrors", typeMonitors.stream()
                .mapToLong(ConnectionHealthMonitor::getErrorsCount).sum());
            typeStats.put("connections", typeMonitors.stream()
                .map(ConnectionHealthMonitor::getMinimalStats)
                .collect(Collectors.toList()));

            result.put(type, typeStats);
        }

        return result;
    }

    /**
     * Check overall health.
     */
    public boolean isOverallHealthy() {
        return monitors.values().stream()
            .allMatch(m -> m.isHealthy(DEFAULT_STALL_THRESHOLD));
    }

    /**
     * Get health summary.
     */
    public Map<String, Object> getHealthSummary() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", isOverallHealthy() ? "UP" : "DOWN");
        result.put("total", monitors.size());
        result.put("healthy", monitors.values().stream()
            .filter(m -> m.isHealthy(DEFAULT_STALL_THRESHOLD)).count());

        List<String> unhealthyNames = monitors.values().stream()
            .filter(m -> !m.isHealthy(DEFAULT_STALL_THRESHOLD))
            .map(m -> m.getName() + " (" + m.getState() + ")")
            .collect(Collectors.toList());
        if (!unhealthyNames.isEmpty()) {
            result.put("unhealthy", unhealthyNames);
        }

        return result;
    }
}
