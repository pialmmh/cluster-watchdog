package com.telcobright.watchdog.rest;

import com.telcobright.watchdog.action.*;
import com.telcobright.watchdog.model.ContainerStatus;
import com.telcobright.watchdog.service.*;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.Map;

@Path("/api/actions")
@Produces(MediaType.APPLICATION_JSON)
public class ActionResource {

    @Inject
    CommandExecutor executor;

    @Inject
    NetworkMonitor networkMonitor;

    @Inject
    DockerMonitor dockerMonitor;

    @Inject
    LxcMonitor lxcMonitor;

    @Inject
    WireGuardMonitor wireGuardMonitor;

    @Inject
    SnapManager snapManager;

    @POST
    @Path("/add-ip")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> addSecondaryIp(Map<String, String> request) {
        String iface = request.get("interface");
        String ip = request.get("ip");

        if (iface == null || ip == null) {
            return Map.of("error", "interface and ip required");
        }

        var action = new AddSecondaryIpAction(networkMonitor, iface, ip);
        var result = action.execute();

        return Map.of(
                "action", action.getDescription(),
                "needed", action.isNeeded(),
                "success", result.isSuccess(),
                "message", result.isSuccess() ? result.stdout() : result.stderr(),
                "executedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/restart-service")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> restartService(Map<String, String> request) {
        String serviceName = request.get("service");

        if (serviceName == null) {
            return Map.of("error", "service name required");
        }

        var action = new RestartServiceAction(executor, serviceName);
        var result = action.execute();

        return Map.of(
                "action", action.getDescription(),
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "Service restarted" : result.stderr(),
                "executedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/hold-snap")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> holdSnapRefresh(Map<String, String> request) {
        String snapName = request.getOrDefault("snap", "lxd");

        var action = new HoldSnapRefreshAction(snapManager, snapName);
        var result = action.execute();

        return Map.of(
                "action", action.getDescription(),
                "needed", action.isNeeded(),
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "Refresh held" : result.stderr(),
                "executedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/restart-container")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> restartContainer(Map<String, String> request) {
        String containerName = request.get("container");
        String typeStr = request.getOrDefault("type", "docker");

        if (containerName == null) {
            return Map.of("error", "container name required");
        }

        ContainerStatus.ContainerType type = "lxc".equalsIgnoreCase(typeStr)
                ? ContainerStatus.ContainerType.LXC
                : ContainerStatus.ContainerType.DOCKER;

        var action = new RestartContainerAction(dockerMonitor, lxcMonitor, containerName, type);
        var result = action.execute();

        return Map.of(
                "action", action.getDescription(),
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "Container restarted" : result.stderr(),
                "executedAt", Instant.now().toString()
        );
    }

    @POST
    @Path("/start-wireguard")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> startWireGuard(Map<String, String> request) {
        String interfaceName = request.getOrDefault("interface", "wg0");

        var action = new StartWireGuardAction(wireGuardMonitor, interfaceName);
        var result = action.execute();

        return Map.of(
                "action", action.getDescription(),
                "needed", action.isNeeded(),
                "success", result.isSuccess(),
                "message", result.isSuccess() ? "Interface started" : result.stderr(),
                "executedAt", Instant.now().toString()
        );
    }
}
