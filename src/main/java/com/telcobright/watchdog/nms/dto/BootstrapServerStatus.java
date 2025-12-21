package com.telcobright.watchdog.nms.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Status of an individual bootstrap server.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BootstrapServerStatus {
    private String server;

    @JsonIgnore
    private String host;

    @JsonIgnore
    private int port;

    private String status;

    @JsonIgnore
    private Integer brokerId;
}
