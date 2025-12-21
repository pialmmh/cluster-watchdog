package com.telcobright.watchdog.nms.startup;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.nms.config.NmsConfig;
import com.telcobright.watchdog.nms.dto.MySQLInstanceInfo;
import com.telcobright.watchdog.nms.service.KafkaMonitoringService;
import com.telcobright.watchdog.nms.service.MySQLMonitoringService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.util.List;

/**
 * NMS startup and shutdown lifecycle handler.
 */
@ApplicationScoped
public class NmsStartup {

    @Inject
    ClusterConfig clusterConfig;

    @Inject
    NmsConfig nmsConfig;

    @Inject
    KafkaMonitoringService kafkaMonitoringService;

    @Inject
    MySQLMonitoringService mysqlMonitoringService;

    void onStart(@Observes StartupEvent event) {
        if (!nmsConfig.enabled()) {
            System.out.println("[NMS] Skipped - nms.enabled=false");
            return;
        }

        System.out.println("========================================");
        System.out.println("  NMS - Network Monitoring Service");
        System.out.println("========================================");
        System.out.println();
        System.out.println("[NMS] Tenant: " + clusterConfig.tenant());

        // Kafka Monitoring
        String bootstrapServers = clusterConfig.kafka().bootstrapServers();
        System.out.println("[NMS-Kafka] Bootstrap servers: " + bootstrapServers);

        // MySQL Monitoring
        List<MySQLInstanceInfo> instances = mysqlMonitoringService.listInstances();
        if (instances.isEmpty()) {
            System.out.println("[NMS-MySQL] No MySQL instances configured");
        } else {
            System.out.println("[NMS-MySQL] Discovered " + instances.size() + " MySQL instance(s):");
            instances.forEach(info -> {
                System.out.println("[NMS-MySQL]   - " + info.getInstanceId());
                System.out.println("[NMS-MySQL]     Database: " + info.getHost() + ":" + info.getPort() + "/" + info.getDatabase());
            });
        }

        System.out.println();
        System.out.println("[NMS] Monitoring API endpoints:");
        System.out.println();
        System.out.println("  Kafka Monitoring:");
        System.out.println("    GET  /api/nms/kafka/cluster           - Cluster info");
        System.out.println("    GET  /api/nms/kafka/overview          - Cluster overview");
        System.out.println("    GET  /api/nms/kafka/monitoring        - Full monitoring data");
        System.out.println("    GET  /api/nms/kafka/brokers           - Broker status");
        System.out.println("    GET  /api/nms/kafka/topics            - Topic health");
        System.out.println("    GET  /api/nms/kafka/consumer-groups   - Consumer groups");
        System.out.println("    GET  /api/nms/kafka/consumer-lag      - Consumer lag");
        System.out.println("    GET  /api/nms/kafka/consumer-lag-by-topic  - Lag by topic");
        System.out.println("    GET  /api/nms/kafka/connections       - Active connections");
        System.out.println("    GET  /api/nms/kafka/storage           - Cluster storage");
        System.out.println("    POST /api/nms/kafka/reset             - Reset connection");
        System.out.println();
        System.out.println("  MySQL Monitoring:");
        System.out.println("    GET  /api/nms/mysql/instances         - List instances");
        System.out.println("    GET  /api/nms/mysql/instances/health  - Health summary");
        System.out.println("    GET  /api/nms/mysql/instances/{id}    - Instance health");
        System.out.println();
        System.out.println("  Connection Health:");
        System.out.println("    GET  /api/nms/connections             - All connection stats");
        System.out.println("    GET  /api/nms/connections/health      - Health summary");
        System.out.println("    GET  /api/nms/connections/minimal     - Minimal stats");
        System.out.println("    GET  /api/nms/connections/by-type     - Stats by type");
        System.out.println("========================================");
    }

    void onStop(@Observes ShutdownEvent event) {
        if (!nmsConfig.enabled()) {
            return;
        }
        System.out.println("[NMS] Shutting down NMS service...");
        kafkaMonitoringService.shutdown();
        System.out.println("[NMS] NMS service stopped");
    }
}
