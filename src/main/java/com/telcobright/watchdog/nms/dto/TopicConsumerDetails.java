package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Consumer group details for a specific topic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicConsumerDetails {
    private String consumerGroupId;
    private String status;
    private int memberCount;
    private long totalLag;
    private List<ConsumerPartitionLag> partitions;
}
