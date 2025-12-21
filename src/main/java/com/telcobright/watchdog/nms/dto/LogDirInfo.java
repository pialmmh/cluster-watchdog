package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Information about a Kafka log directory.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogDirInfo {
    private String path;
    private long usedBytes;
    private String usedFormatted;
    private String error;
    private List<TopicPartitionStorage> partitions;
}
