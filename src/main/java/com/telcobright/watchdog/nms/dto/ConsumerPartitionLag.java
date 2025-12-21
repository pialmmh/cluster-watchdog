package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Lag information for a specific partition consumed by a consumer group.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerPartitionLag {
    private int partition;
    private long currentOffset;
    private long endOffset;
    private long lag;
}
