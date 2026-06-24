package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * System metrics (CPU, Memory) for a Kafka broker from node_exporter.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerSystemMetrics {
    private int brokerId;
    private String host;
    private int port;
    private String status;

    // Memory metrics
    private Long memoryTotalBytes;
    private String memoryTotalFormatted;
    private Long memoryAvailableBytes;
    private String memoryAvailableFormatted;
    private Long memoryUsedBytes;
    private String memoryUsedFormatted;
    private Double memoryUsedPercent;

    // CPU metrics
    private Integer cpuCores;
    private Double cpuUsagePercent;
    private Double loadAverage1min;
    private Double loadAverage5min;
    private Double loadAverage15min;
}
