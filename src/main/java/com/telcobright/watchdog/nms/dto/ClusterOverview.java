package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Cluster overview metrics.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterOverview {
    private String clusterId;
    private String clusterStatus;
    private int totalBrokers;
    private int onlineBrokers;
    private int upBrokers;
    private int downBrokers;
    private int controllerId;
    private String kafkaVersion;
    private String tenantName;
    private String profile;
    private String bootstrapServers;
    private List<BootstrapServerStatus> brokerStatuses;
}
