package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Summary of all consumer groups consuming from a specific topic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicConsumerLagSummary {
    private String topicName;
    private int consumerCount;
    private long totalLag;
    private List<TopicConsumerDetails> consumers;
}
