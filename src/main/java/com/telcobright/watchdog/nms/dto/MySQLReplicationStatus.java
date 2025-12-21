package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MySQL replication status (for slave instances).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MySQLReplicationStatus {
    private String slaveIOState;
    private String masterHost;
    private int masterPort;
    private String masterUser;
    private boolean slaveIORunning;
    private boolean slaveSQLRunning;
    private long secondsBehindMaster;
    private String lastError;
    private boolean replicationHealthy;
}
