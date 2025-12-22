package com.telcobright.watchdog.nms.api;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.nms.dto.*;
import com.telcobright.watchdog.nms.service.KafkaMonitoringService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * REST API endpoint for Kafka cluster monitoring.
 */
@ApplicationScoped
@Path("/api/v1/nms/kafka")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaMonitoringEndpoint {

    @Inject
    KafkaMonitoringService monitoringService;

    @Inject
    ClusterConfig clusterConfig;

    /**
     * Get cluster info.
     */
    @GET
    @Path("/clusters/info")
    public Response getClusterInfo() {
        KafkaClusterInfo info = monitoringService.getClusterInfo();
        return Response.ok(info).build();
    }

    /**
     * Get complete monitoring data for the cluster.
     */
    @GET
    @Path("/clusters/monitoring")
    public Response getClusterMonitoring() {
        KafkaMonitoringResponse monitoring = monitoringService.getClusterMonitoring();

        if (monitoring.getError() != null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(monitoring)
                    .build();
        }

        return Response.ok(monitoring).build();
    }

    /**
     * Get cluster overview.
     */
    @GET
    @Path("/clusters/overview")
    public Response getClusterOverview() {
        ClusterOverview overview = monitoringService.getClusterOverview();
        // Wrap response with tenant name as key for UI compatibility
        return Response.ok(Map.of(overview.getTenantName(), overview)).build();
    }

    /**
     * Get cluster health.
     */
    @GET
    @Path("/clusters/health")
    public Response getClusterHealth() {
        ClusterOverview overview = monitoringService.getClusterOverview();
        // Wrap response with tenant name as key for UI compatibility
        return Response.ok(Map.of(overview.getTenantName(), overview)).build();
    }

    /**
     * Get broker status.
     */
    @GET
    @Path("/brokers")
    public Response getBrokerStatus() {
        List<BrokerStatus> brokers = monitoringService.getBrokerStatus();
        return Response.ok(brokers).build();
    }

    /**
     * Get topic health.
     */
    @GET
    @Path("/topics")
    public Response getTopicHealth() {
        List<TopicHealth> topics = monitoringService.getTopicHealth();
        return Response.ok(topics).build();
    }

    /**
     * Get consumer groups.
     */
    @GET
    @Path("/consumer-groups")
    public Response getConsumerGroups() {
        List<ConsumerGroupInfo> groups = monitoringService.getConsumerGroups();
        return Response.ok(groups).build();
    }

    /**
     * Get consumer lag.
     */
    @GET
    @Path("/consumer-lag")
    public Response getConsumerLags() {
        List<ConsumerLag> lags = monitoringService.getConsumerLags();
        return Response.ok(lags).build();
    }

    /**
     * Get consumer lag organized by topic.
     */
    @GET
    @Path("/consumer-lag-by-topic")
    public Response getConsumerLagByTopic() {
        List<TopicConsumerLagSummary> summary = monitoringService.getConsumerLagByTopic();
        // Wrap response with tenant name as key for UI compatibility
        return Response.ok(Map.of(clusterConfig.tenant(), summary)).build();
    }

    /**
     * Get active connections.
     */
    @GET
    @Path("/connections")
    public Response getActiveConnections() {
        List<ActiveConnection> connections = monitoringService.getActiveConnections();
        return Response.ok(connections).build();
    }

    /**
     * Get cluster storage.
     */
    @GET
    @Path("/clusters/storage")
    public Response getClusterStorage() {
        ClusterStorage storage = monitoringService.getClusterStorage();

        if (storage.getError() != null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(storage)
                    .build();
        }

        return Response.ok(storage).build();
    }

    /**
     * Reset the admin client connection.
     */
    @POST
    @Path("/clusters/reset")
    public Response resetConnection() {
        monitoringService.resetClient();
        return Response.ok(Map.of(
                "status", "success",
                "message", "Kafka admin client connection reset"
        )).build();
    }
}
