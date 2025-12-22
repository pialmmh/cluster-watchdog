package com.telcobright.watchdog.nms.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a MySQL instance configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MySQLInstanceInfo {
    private String instanceId;
    private String tenantName;
    private String profile;
    private String host;
    private int port;
    private String database;
    private String username;

    @JsonIgnore
    private String password;

    private String role;
    private boolean enabled;
}
