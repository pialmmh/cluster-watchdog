package com.telcobright.watchdog.model;

import java.time.Instant;
import java.util.List;

public record ClusterHealth(
        String clusterName,
        OverallStatus status,
        int totalNodes,
        int healthyNodes,
        int degradedNodes,
        int unhealthyNodes,
        KafkaClusterStatus kafka,
        List<NodeStatus> nodes,
        List<String> alerts,
        Instant checkedAt
) {
    public enum OverallStatus {
        HEALTHY, DEGRADED, CRITICAL, UNKNOWN
    }

    public record KafkaClusterStatus(
            int totalBrokers,
            int availableBrokers,
            boolean controllerId,
            boolean topicsAccessible,
            String message
    ) {}

    public static ClusterHealth healthy(String name, List<NodeStatus> nodes, KafkaClusterStatus kafka) {
        return new ClusterHealth(name, OverallStatus.HEALTHY, nodes.size(), nodes.size(),
                0, 0, kafka, nodes, List.of(), Instant.now());
    }
}
