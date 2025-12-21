package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * MySQL master status (for master instances).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MySQLMasterStatus {
    private String binlogFile;
    private long binlogPosition;
    private List<SlaveInfo> connectedSlaves;
    private int slaveCount;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SlaveInfo {
        private int serverId;
        private String host;
        private int port;
        private String uuid;
    }
}
