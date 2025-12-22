package com.telcobright.watchdog.nms.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;
import java.util.Optional;

/**
 * NMS (Network Monitoring Service) configuration.
 * Reads from active tenant properties (application-{tenant}.properties).
 */
@ConfigMapping(prefix = "nms")
public interface NmsConfig {

    /**
     * Whether NMS is enabled.
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Kafka monitoring configuration.
     */
    KafkaConfig kafka();

    /**
     * MySQL instances to monitor.
     */
    Optional<List<MySQLInstanceConfig>> mysql();

    interface KafkaConfig {
        /**
         * Kafka cluster ID for identification.
         */
        @WithDefault("default")
        String clusterId();

        /**
         * Environment profile (dev, prod, etc).
         */
        Optional<String> profile();

        /**
         * Bootstrap servers for Kafka cluster.
         */
        Optional<String> bootstrapServers();

        /**
         * Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
         */
        @WithDefault("PLAINTEXT")
        String securityProtocol();
    }

    interface MySQLInstanceConfig {
        /**
         * Instance identifier.
         */
        String id();

        /**
         * Environment profile (dev, prod, etc).
         */
        Optional<String> profile();

        /**
         * MySQL host.
         */
        String host();

        /**
         * MySQL port.
         */
        @WithDefault("3306")
        int port();

        /**
         * Database name.
         */
        String database();

        /**
         * MySQL username.
         */
        String username();

        /**
         * MySQL password.
         */
        Optional<String> password();

        /**
         * Role: master, slave, standalone.
         */
        @WithDefault("standalone")
        String role();
    }
}
