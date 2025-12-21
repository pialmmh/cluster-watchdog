package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Storage information for a topic partition.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionStorage {
    private String topic;
    private int partition;
    private long sizeBytes;
    private String sizeFormatted;
    private long offsetLag;
}
