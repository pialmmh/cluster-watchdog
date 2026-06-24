package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * System metrics for all brokers in the cluster.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterSystemMetrics {
    private String clusterId;
    private int brokerCount;
    private List<BrokerSystemMetrics> brokers;
    private String error;
}
