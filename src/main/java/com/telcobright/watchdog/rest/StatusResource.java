package com.telcobright.watchdog.rest;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.model.BgpRoutingStatus;
import com.telcobright.watchdog.model.ClusterHealth;
import com.telcobright.watchdog.scheduler.ClusterMonitorScheduler;
import com.telcobright.watchdog.service.*;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/api/status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource {

    @Inject
    DockerMonitor dockerMonitor;

    @Inject
    LxcMonitor lxcMonitor;

    @Inject
    WireGuardMonitor wireGuardMonitor;

    @Inject
    NetworkMonitor networkMonitor;

    @Inject
    SnapManager snapManager;

    @Inject
    BgpMonitor bgpMonitor;

    @Inject
    ClusterConfig config;

    @Inject
    ClusterMonitorScheduler scheduler;

    @GET
    public Map<String, Object> getOverallStatus() {
        List<String> alerts = new ArrayList<>();

        boolean dockerOk = dockerMonitor.isDockerRunning();
        boolean lxdOk = lxcMonitor.isLxdRunning();

        if (!dockerOk) {
            alerts.add("Docker daemon not running");
        }
        if (!lxdOk) {
            alerts.add("LXD daemon not running");
        }

        String overallStatus = alerts.isEmpty() ? "HEALTHY" : "DEGRADED";

        return Map.of(
                "status", overallStatus,
                "docker", dockerOk,
                "lxd", lxdOk,
                "alerts", alerts,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/docker")
    public Map<String, Object> getDockerStatus() {
        return Map.of(
                "running", dockerMonitor.isDockerRunning(),
                "containers", dockerMonitor.listContainers(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/lxc")
    public Map<String, Object> getLxcStatus() {
        return Map.of(
                "running", lxcMonitor.isLxdRunning(),
                "containers", lxcMonitor.listContainers(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/wireguard/{interface}")
    public Map<String, Object> getWireGuardStatus(String interfaceName) {
        var status = wireGuardMonitor.checkInterface(interfaceName);
        var peers = wireGuardMonitor.getPeers(interfaceName);

        return Map.of(
                "interface", interfaceName,
                "status", status,
                "peers", peers,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bridge/{name}")
    public Map<String, Object> getBridgeStatus(String name) {
        var status = networkMonitor.checkBridge(name);

        return Map.of(
                "bridge", name,
                "status", status,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/snaps")
    public Map<String, Object> getSnapStatus() {
        var snaps = snapManager.listSnaps();
        var pending = snapManager.getPendingRefreshes();
        boolean lxdHeld = snapManager.isRefreshHeld("lxd");

        return Map.of(
                "snaps", snaps,
                "pendingRefreshes", pending,
                "lxdRefreshHeld", lxdHeld,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bgp")
    public Map<String, Object> getBgpStatus() {
        String frrContainer = config.local().frrContainer().orElse(null);

        // Get fresh status
        BgpRoutingStatus routingStatus = bgpMonitor.getRoutingStatus(frrContainer);

        return Map.of(
                "nodeId", config.local().nodeId(),
                "frrRunning", routingStatus.frrRunning(),
                "health", routingStatus.health().name(),
                "message", routingStatus.message(),
                "totalPeers", routingStatus.totalPeers(),
                "establishedPeers", routingStatus.establishedPeers(),
                "totalRoutes", routingStatus.totalRoutes(),
                "routeChecks", routingStatus.routeChecks(),
                "missingRoutes", routingStatus.missingRoutes(),
                "checkedAt", routingStatus.checkedAt().toString()
        );
    }

    @GET
    @Path("/bgp/routes")
    public Map<String, Object> getBgpRoutes() {
        String frrContainer = config.local().frrContainer().orElse(null);

        var bgpRoutes = bgpMonitor.getRoutes(frrContainer);
        var kernelRoutes = bgpMonitor.getAllRoutes(frrContainer);
        var neighbors = bgpMonitor.getNeighbors(frrContainer);

        return Map.of(
                "nodeId", config.local().nodeId(),
                "bgpRoutes", bgpRoutes,
                "kernelRoutes", kernelRoutes,
                "neighbors", neighbors,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/bgp/cached")
    public Map<String, Object> getCachedBgpStatus() {
        var lastStatus = scheduler.getLastBgpRoutingStatus();
        if (lastStatus == null) {
            return Map.of(
                    "available", false,
                    "message", "No BGP status check has run yet"
            );
        }

        return Map.of(
                "available", true,
                "nodeId", config.local().nodeId(),
                "health", lastStatus.health().name(),
                "message", lastStatus.message(),
                "totalPeers", lastStatus.totalPeers(),
                "establishedPeers", lastStatus.establishedPeers(),
                "totalRoutes", lastStatus.totalRoutes(),
                "routeChecks", lastStatus.routeChecks(),
                "missingRoutes", lastStatus.missingRoutes(),
                "checkedAt", lastStatus.checkedAt().toString()
        );
    }
}
