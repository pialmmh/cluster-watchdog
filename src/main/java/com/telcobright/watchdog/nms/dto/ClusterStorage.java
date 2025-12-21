package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Complete storage information for a Kafka cluster.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterStorage {
    private String clusterId;
    private long totalUsedBytes;
    private String totalUsedFormatted;
    private int brokerCount;
    private List<BrokerStorage> brokers;
    private String error;
}
