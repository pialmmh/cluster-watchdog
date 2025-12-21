package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Topic partition health information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicHealth {
    private String topicName;
    private int partitions;
    private int underReplicatedPartitions;
    private int offlinePartitions;
    private int replicationFactor;
    private boolean internal;
}
