package com.telcobright.watchdog.model;

import java.time.Instant;
import java.util.List;

public record NodeStatus(
        int nodeId,
        String hostIp,
        boolean reachable,
        NetworkStatus wireGuard,
        NetworkStatus bridge,
        BgpRoutingStatus bgpRouting,
        List<ContainerStatus> dockerContainers,
        List<ContainerStatus> lxcContainers,
        List<PeerConnectivity> peerConnectivity,
        OverallHealth health,
        Instant checkedAt
) {
    public enum OverallHealth {
        HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN
    }

    public record PeerConnectivity(
            String peerIp,
            boolean reachable,
            double packetLossPercent,
            double latencyMs
    ) {}

    public static NodeStatus unreachable(int nodeId, String hostIp) {
        return new NodeStatus(nodeId, hostIp, false, null, null, null,
                List.of(), List.of(), List.of(), OverallHealth.UNHEALTHY, Instant.now());
    }
}
