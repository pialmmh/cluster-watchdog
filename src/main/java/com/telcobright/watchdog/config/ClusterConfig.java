package com.telcobright.watchdog.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "cluster")
public interface ClusterConfig {

    String tenant();

    String name();

    @WithDefault("true")
    boolean autoHealEnabled();

    /** Local node config - agent fixes these */
    LocalConfig local();

    SnapConfig snap();

    KafkaConfig kafka();

    /** BGP/routing configuration */
    Optional<BgpConfig> bgp();

    /** All nodes in cluster - for REST API queries */
    Map<String, NodeConfig> nodes();

    interface BgpConfig {
        /** WireGuard overlay subnet (e.g., 10.9.9.0/24) */
        Optional<String> wireguardSubnet();

        /** VPN client subnet for developer access (e.g., 10.8.0.0/24) */
        Optional<String> vpnClientSubnet();

        /** Additional custom routes that should be present */
        Optional<List<String>> additionalExpectedRoutes();
    }

    interface LocalConfig {
        int nodeId();

        @WithDefault("wg0")
        String wireGuardInterface();

        @WithDefault("lxdbr0")
        String bridge();

        Optional<String> overlayIp();

        Optional<String> frrContainer();

        /** Systemd service that persists Kafka IP on bridge */
        @WithDefault("lxdbr0-kafka-ip")
        String kafkaIpService();

        /** Critical systemd services to monitor */
        Optional<List<String>> criticalServices();

        /** Expected BGP routes - container subnets this node must reach */
        Optional<List<String>> expectedContainerSubnets();

        /** Expected WireGuard subnet routes */
        Optional<List<String>> expectedWireguardSubnets();
    }

    interface SnapConfig {
        @WithDefault("true")
        boolean holdLxdRefresh();
    }

    interface KafkaConfig {
        String bootstrapServers();

        @WithDefault("3")
        int expectedBrokers();
    }

    interface NodeConfig {
        int id();

        String hostIp();

        @WithDefault("22")
        int sshPort();

        Optional<String> overlayIp();

        Optional<String> bridgeIp();

        /** Container subnet for this node (e.g., 10.10.199.0/24) */
        Optional<String> containerSubnet();

        Optional<List<ServiceConfig>> services();
    }

    interface ServiceConfig {
        String name();

        String type();  // docker, lxc

        String container();

        int port();
    }
}
