package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Complete Kafka monitoring response containing all metrics.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMonitoringResponse {
    private ClusterOverview clusterOverview;
    private List<BrokerStatus> brokers;
    private List<TopicHealth> topics;
    private List<ConsumerGroupInfo> consumerGroups;
    private List<ConsumerLag> consumerLags;
    private List<ActiveConnection> activeConnections;
    private Instant timestamp;
    private String error;
}
