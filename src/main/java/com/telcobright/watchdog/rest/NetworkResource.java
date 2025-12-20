package com.telcobright.watchdog.rest;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.model.NodeStatus;
import com.telcobright.watchdog.service.BgpMonitor;
import com.telcobright.watchdog.service.NetworkMonitor;
import com.telcobright.watchdog.service.WireGuardMonitor;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/api/network")
@Produces(MediaType.APPLICATION_JSON)
public class NetworkResource {

    @Inject
    NetworkMonitor networkMonitor;

    @Inject
    WireGuardMonitor wireGuardMonitor;

    @Inject
    BgpMonitor bgpMonitor;

    @Inject
    ClusterConfig config;

    @GET
    @Path("/bridge/{name}")
    public Map<String, Object> checkBridge(@PathParam("name") String bridgeName) {
        var status = networkMonitor.checkBridge(bridgeName);

        return Map.of(
                "bridge", bridgeName,
                "status", status,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bridge/{name}/ip/{ip}")
    public Map<String, Object> checkBridgeIp(@PathParam("name") String bridgeName,
                                              @PathParam("ip") String ipAddress) {
        boolean hasIp = networkMonitor.hasIpAddress(bridgeName, ipAddress);

        return Map.of(
                "bridge", bridgeName,
                "ip", ipAddress,
                "present", hasIp,
                "checkedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/bridge/{name}/ip")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> addBridgeIp(@PathParam("name") String bridgeName,
                                            Map<String, String> request) {
        String ipAddress = request.get("ip");
        if (ipAddress == null || ipAddress.isEmpty()) {
            return Map.of("error", "IP address required");
        }

        var result = networkMonitor.addIpAddress(bridgeName, ipAddress);

        return Map.of(
                "bridge", bridgeName,
                "ip", ipAddress,
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "IP added" : result.stderr(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/wireguard/{iface}")
    public Map<String, Object> checkWireGuard(@PathParam("iface") String interfaceName) {
        var status = wireGuardMonitor.checkInterface(interfaceName);
        var peers = wireGuardMonitor.getPeers(interfaceName);

        return Map.of(
                "interface", interfaceName,
                "status", status,
                "peers", peers,
                "peerCount", peers.size(),
                "activePeers", peers.stream().filter(WireGuardMonitor.PeerInfo::isActive).count(),
                "checkedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/wireguard/{iface}/restart")
    public Map<String, Object> restartWireGuard(@PathParam("iface") String interfaceName) {
        var result = wireGuardMonitor.restartInterface(interfaceName);

        return Map.of(
                "interface", interfaceName,
                "action", "restart",
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "Interface restarted" : result.stderr(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bgp")
    public Map<String, Object> checkBgp() {
        String frrContainer = config.local().frrContainer().orElse(null);
        var status = bgpMonitor.getStatus(frrContainer);

        return Map.of(
                "frrContainer", frrContainer != null ? frrContainer : "local",
                "status", status,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bgp/neighbors")
    public Map<String, Object> getBgpNeighbors() {
        String frrContainer = config.local().frrContainer().orElse(null);
        var neighbors = bgpMonitor.getNeighbors(frrContainer);

        return Map.of(
                "frrContainer", frrContainer != null ? frrContainer : "local",
                "neighbors", neighbors,
                "count", neighbors.size(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bgp/routes")
    public Map<String, Object> getBgpRoutes() {
        String frrContainer = config.local().frrContainer().orElse(null);
        var routes = bgpMonitor.getRoutes(frrContainer);

        return Map.of(
                "frrContainer", frrContainer != null ? frrContainer : "local",
                "routes", routes,
                "count", routes.size(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/ping/{ip}")
    public Map<String, Object> pingHost(@PathParam("ip") String ipAddress) {
        NodeStatus.PeerConnectivity result = networkMonitor.pingPeer(ipAddress);

        return Map.of(
                "target", ipAddress,
                "reachable", result.reachable(),
                "packetLoss", result.packetLossPercent(),
                "latencyMs", result.latencyMs(),
                "checkedAt", Instant.now().toString()
        );
    }

    /** Check connectivity to all cluster nodes */
    @GET
    @Path("/connectivity")
    public Map<String, Object> checkConnectivity() {
        List<Map<String, Object>> results = new ArrayList<>();

        for (var entry : config.nodes().entrySet()) {
            var node = entry.getValue();
            String overlayIp = node.overlayIp().orElse(null);
            if (overlayIp != null && !overlayIp.isEmpty()) {
                var ping = networkMonitor.pingPeer(overlayIp);
                results.add(Map.of(
                        "nodeKey", entry.getKey(),
                        "nodeId", node.id(),
                        "ip", overlayIp,
                        "reachable", ping.reachable(),
                        "latencyMs", ping.latencyMs()
                ));
            }
        }

        long reachable = results.stream()
                .filter(r -> (Boolean) r.get("reachable"))
                .count();

        return Map.of(
                "localNodeId", config.local().nodeId(),
                "nodes", results,
                "totalNodes", results.size(),
                "reachableNodes", reachable,
                "status", reachable == results.size() ? "HEALTHY" : "DEGRADED",
                "checkedAt", Instant.now().toString()
        );
    }
}
