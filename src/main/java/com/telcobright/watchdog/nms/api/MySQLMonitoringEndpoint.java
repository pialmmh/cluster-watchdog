package com.telcobright.watchdog.nms.api;

import com.telcobright.watchdog.nms.dto.MySQLHealthStatus;
import com.telcobright.watchdog.nms.dto.MySQLInstanceInfo;
import com.telcobright.watchdog.nms.service.MySQLMonitoringService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * REST API endpoint for MySQL database monitoring.
 */
@ApplicationScoped
@Path("/api/v1/nms/mysql")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MySQLMonitoringEndpoint {

    @Inject
    MySQLMonitoringService monitoringService;

    /**
     * List all configured MySQL instances.
     */
    @GET
    @Path("/instances")
    public Response listInstances() {
        List<MySQLInstanceInfo> instances = monitoringService.listInstances();
        return Response.ok(instances).build();
    }

    /**
     * Get health summary for all MySQL instances.
     */
    @GET
    @Path("/instances/health")
    public Response getAllInstancesHealth() {
        Map<String, MySQLHealthStatus> health = monitoringService.getAllInstancesHealth();
        return Response.ok(health).build();
    }

    /**
     * Get health status for a specific MySQL instance.
     */
    @GET
    @Path("/instances/{instanceId}")
    public Response getInstanceHealth(@PathParam("instanceId") String instanceId) {
        MySQLHealthStatus health = monitoringService.getInstanceHealthById(instanceId);

        if ("NOT_FOUND".equals(health.getStatus())) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(health)
                    .build();
        }

        return Response.ok(health).build();
    }
}
