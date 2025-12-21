package com.telcobright.watchdog.nms.api;

import com.telcobright.watchdog.nms.health.ConnectionHealthRegistry;
import io.smallrye.common.annotation.NonBlocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

/**
 * REST API for connection health monitoring.
 */
@Path("/api/nms/connections")
@Produces(MediaType.APPLICATION_JSON)
public class ConnectionHealthEndpoint {

    @Inject
    ConnectionHealthRegistry registry;

    /**
     * Health summary.
     */
    @GET
    @Path("/health")
    @NonBlocking
    public Response getHealthSummary() {
        Map<String, Object> health = registry.getHealthSummary();
        String status = (String) health.get("status");
        int httpStatus = "UP".equals(status) ? 200 : 503;
        return Response.status(httpStatus).entity(health).build();
    }

    /**
     * Full stats for all connections.
     */
    @GET
    @NonBlocking
    public Response getAllStats() {
        return Response.ok(registry.getAllStats()).build();
    }

    /**
     * Minimal stats for all connections.
     */
    @GET
    @Path("/minimal")
    @NonBlocking
    public Response getMinimalStats() {
        return Response.ok(registry.getMinimalStats()).build();
    }

    /**
     * Stats grouped by connection type.
     */
    @GET
    @Path("/by-type")
    @NonBlocking
    public Response getStatsByType() {
        return Response.ok(registry.getStatsByType()).build();
    }

    /**
     * Stats for a specific connection.
     */
    @GET
    @Path("/{name}")
    @NonBlocking
    public Response getConnectionStats(@PathParam("name") String name) {
        var monitor = registry.get(name);
        if (monitor == null) {
            return Response.status(404)
                .entity(Map.of("error", "Connection not found: " + name))
                .build();
        }
        return Response.ok(monitor.getStats()).build();
    }
}
