package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Consumer lag per partition.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerLag {
    private String groupId;
    private String topic;
    private int partition;
    private long currentOffset;
    private long endOffset;
    private long lag;
}
