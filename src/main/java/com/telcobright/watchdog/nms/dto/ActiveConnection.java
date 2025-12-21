package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Active client connection information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ActiveConnection {
    private String clientId;
    private String clientType;
    private String clientHost;
    private Instant connectedSince;
    private String connectedSinceFormatted;
}
