package com.telcobright.watchdog.nms.service;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.nms.config.NmsConfig;
import com.telcobright.watchdog.nms.dto.*;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;

/**
 * Service for monitoring the Kafka cluster configured in the active tenant.
 * Simplified for single-tenant deployment.
 */
@ApplicationScoped
public class KafkaMonitoringService {

    private static final Logger LOG = Logger.getLogger(KafkaMonitoringService.class);

    @Inject
    ClusterConfig clusterConfig;

    @Inject
    NmsConfig nmsConfig;

    private volatile KafkaAdminClientWrapper adminClient;
    private volatile KafkaClusterInfo clusterInfo;

    /**
     * Get the Kafka cluster info.
     */
    public KafkaClusterInfo getClusterInfo() {
        ensureInitialized();
        return clusterInfo;
    }

    /**
     * Get complete monitoring data for the cluster.
     */
    public KafkaMonitoringResponse getClusterMonitoring() {
        try {
            ensureInitialized();
            String profile = nmsConfig.kafka().profile().orElse(null);

            return KafkaMonitoringResponse.builder()
                    .clusterOverview(adminClient.getClusterOverview(clusterConfig.tenant(), profile))
                    .brokers(adminClient.getBrokerStatus())
                    .topics(adminClient.getTopicHealth())
                    .consumerGroups(adminClient.getConsumerGroups())
                    .consumerLags(adminClient.getConsumerLags())
                    .activeConnections(adminClient.getActiveConnections())
                    .timestamp(Instant.now())
                    .build();

        } catch (Exception e) {
            LOG.error("[NMS] Error getting cluster monitoring: " + e.getMessage());
            return KafkaMonitoringResponse.builder()
                    .error("Error connecting to cluster: " + e.getMessage())
                    .timestamp(Instant.now())
                    .build();
        }
    }

    /**
     * Get cluster overview.
     */
    public ClusterOverview getClusterOverview() {
        try {
            ensureInitialized();
            String profile = nmsConfig.kafka().profile().orElse(null);
            return adminClient.getClusterOverview(clusterConfig.tenant(), profile);
        } catch (Exception e) {
            LOG.error("[NMS] Error getting cluster overview: " + e.getMessage());
            return ClusterOverview.builder()
                    .clusterId(clusterConfig.tenant())
                    .clusterStatus("Error: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Get broker status.
     */
    public List<BrokerStatus> getBrokerStatus() {
        try {
            ensureInitialized();
            return adminClient.getBrokerStatus();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting broker status: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get topic health.
     */
    public List<TopicHealth> getTopicHealth() {
        try {
            ensureInitialized();
            return adminClient.getTopicHealth();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting topic health: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get consumer groups.
     */
    public List<ConsumerGroupInfo> getConsumerGroups() {
        try {
            ensureInitialized();
            return adminClient.getConsumerGroups();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting consumer groups: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get consumer lags.
     */
    public List<ConsumerLag> getConsumerLags() {
        try {
            ensureInitialized();
            return adminClient.getConsumerLags();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting consumer lags: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get consumer lag organized by topic.
     */
    public List<TopicConsumerLagSummary> getConsumerLagByTopic() {
        try {
            ensureInitialized();
            return adminClient.getConsumerLagByTopic();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting consumer lag by topic: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get active connections.
     */
    public List<ActiveConnection> getActiveConnections() {
        try {
            ensureInitialized();
            return adminClient.getActiveConnections();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting active connections: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get cluster storage.
     */
    public ClusterStorage getClusterStorage() {
        try {
            ensureInitialized();
            return adminClient.getClusterStorage();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting cluster storage: " + e.getMessage());
            return ClusterStorage.builder()
                    .clusterId(clusterConfig.tenant())
                    .error("Error: " + e.getMessage())
                    .brokers(Collections.emptyList())
                    .build();
        }
    }

    /**
     * Get system metrics (CPU, Memory) for all brokers.
     */
    public ClusterSystemMetrics getSystemMetrics() {
        try {
            ensureInitialized();
            return adminClient.getSystemMetrics();
        } catch (Exception e) {
            LOG.error("[NMS] Error getting system metrics: " + e.getMessage());
            return ClusterSystemMetrics.builder()
                    .clusterId(clusterConfig.tenant())
                    .error("Error: " + e.getMessage())
                    .brokers(Collections.emptyList())
                    .build();
        }
    }

    /**
     * Reset the admin client (useful for reconnection).
     */
    public void resetClient() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
        clusterInfo = null;
        LOG.info("[NMS] Kafka admin client reset");
    }

    /**
     * Check if the cluster is reachable.
     */
    public boolean isReachable() {
        try {
            ensureInitialized();
            return adminClient.isReachable();
        } catch (Exception e) {
            return false;
        }
    }

    private synchronized void ensureInitialized() {
        if (adminClient == null) {
            String tenant = clusterConfig.tenant();
            LOG.infof("[NMS] DEBUG: Read tenant from config: %s", tenant);

            String bootstrapServers = clusterConfig.kafka().bootstrapServers();
            String clusterId = tenant + "-kafka";

            LOG.infof("[NMS] Initializing Kafka admin client for %s: %s", clusterId, bootstrapServers);

            clusterInfo = KafkaClusterInfo.builder()
                    .clusterId(clusterId)
                    .tenantName(clusterConfig.tenant())
                    .bootstrapServers(bootstrapServers)
                    .securityProtocol("PLAINTEXT")
                    .enabled(true)
                    .build();

            adminClient = new KafkaAdminClientWrapper(clusterId, bootstrapServers, "PLAINTEXT");
        }
    }

    @PreDestroy
    public void shutdown() {
        if (adminClient != null) {
            LOG.info("[NMS] Shutting down Kafka admin client");
            adminClient.close();
            adminClient = null;
        }
    }
}
