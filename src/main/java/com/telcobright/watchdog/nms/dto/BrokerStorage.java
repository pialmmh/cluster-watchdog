package com.telcobright.watchdog.nms.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Storage information for a Kafka broker.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerStorage {
    private int brokerId;
    private String host;
    private int port;
    private List<String> storagePaths;

    @JsonIgnore
    private List<LogDirInfo> logDirs;

    private long totalUsedBytes;
    private String totalUsedFormatted;
    private String status;

    // Filesystem information from node_exporter
    private Long filesystemSizeBytes;
    private String filesystemSizeFormatted;
    private Long filesystemAvailBytes;
    private String filesystemAvailFormatted;
    private Long filesystemUsedBytes;
    private String filesystemUsedFormatted;
    private Double filesystemUsedPercent;
    private String mountPoint;
}
