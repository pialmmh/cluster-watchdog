package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a Kafka cluster configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaClusterInfo {
    private String clusterId;
    private String tenantName;
    private String bootstrapServers;
    private String securityProtocol;
    private boolean enabled;
}
