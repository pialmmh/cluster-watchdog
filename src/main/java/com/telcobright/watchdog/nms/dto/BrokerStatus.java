package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Individual broker status information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerStatus {
    private int brokerId;
    private String host;
    private int port;
    private String status;
    private String rack;
    private boolean isController;
}
