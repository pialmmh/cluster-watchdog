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

    /** All nodes in cluster - for REST API queries */
    Map<String, NodeConfig> nodes();

    interface LocalConfig {
        int nodeId();

        @WithDefault("wg0")
        String wireGuardInterface();

        @WithDefault("lxdbr0")
        String bridge();

        Optional<String> overlayIp();

        Optional<String> frrContainer();
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

        Optional<List<ServiceConfig>> services();
    }

    interface ServiceConfig {
        String name();

        String type();  // docker, lxc

        String container();

        int port();
    }
}
