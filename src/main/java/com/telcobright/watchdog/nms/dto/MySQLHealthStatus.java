package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Health status of a MySQL instance.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MySQLHealthStatus {
    private String instanceId;
    private String tenantName;
    private String profile;
    private String host;
    private int port;
    private String database;
    private String role;
    private String status;
    private String version;
    private long uptimeSeconds;
    private int activeConnections;
    private int maxConnections;
    private MySQLReplicationStatus replication;
    private MySQLMasterStatus masterStatus;
    private String error;
}
