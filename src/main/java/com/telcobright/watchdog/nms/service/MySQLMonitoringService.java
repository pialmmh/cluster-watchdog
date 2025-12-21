package com.telcobright.watchdog.nms.service;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.nms.config.NmsConfig;
import com.telcobright.watchdog.nms.dto.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.*;

/**
 * Service for monitoring MySQL instances configured in the active tenant.
 */
@ApplicationScoped
public class MySQLMonitoringService {

    private static final Logger LOG = Logger.getLogger(MySQLMonitoringService.class);

    @Inject
    ClusterConfig clusterConfig;

    @Inject
    NmsConfig nmsConfig;

    /**
     * List all configured MySQL instances.
     */
    public List<MySQLInstanceInfo> listInstances() {
        List<MySQLInstanceInfo> instances = new ArrayList<>();

        if (nmsConfig.mysql().isEmpty()) {
            return instances;
        }

        for (NmsConfig.MySQLInstanceConfig config : nmsConfig.mysql().get()) {
            instances.add(MySQLInstanceInfo.builder()
                    .instanceId(config.id())
                    .tenantName(clusterConfig.tenant())
                    .host(config.host())
                    .port(config.port())
                    .database(config.database())
                    .username(config.username())
                    .password(config.password().orElse(null))
                    .role(config.role())
                    .enabled(true)
                    .build());
        }

        return instances;
    }

    /**
     * Get health status for all MySQL instances.
     */
    public Map<String, MySQLHealthStatus> getAllInstancesHealth() {
        Map<String, MySQLHealthStatus> healthMap = new LinkedHashMap<>();

        for (MySQLInstanceInfo instanceInfo : listInstances()) {
            MySQLHealthStatus health = getInstanceHealth(instanceInfo);
            healthMap.put(instanceInfo.getInstanceId(), health);
        }

        return healthMap;
    }

    /**
     * Get health status for a specific MySQL instance by ID.
     */
    public MySQLHealthStatus getInstanceHealthById(String instanceId) {
        Optional<MySQLInstanceInfo> instanceOpt = listInstances().stream()
                .filter(i -> i.getInstanceId().equals(instanceId))
                .findFirst();

        if (instanceOpt.isEmpty()) {
            return MySQLHealthStatus.builder()
                    .instanceId(instanceId)
                    .status("NOT_FOUND")
                    .error("Instance not found: " + instanceId)
                    .build();
        }

        return getInstanceHealth(instanceOpt.get());
    }

    /**
     * Get health status for a MySQL instance.
     */
    public MySQLHealthStatus getInstanceHealth(MySQLInstanceInfo instanceInfo) {
        try (Connection conn = createConnection(instanceInfo)) {
            return checkHealth(conn, instanceInfo);
        } catch (SQLException e) {
            LOG.warnf("[NMS-MySQL] Connection failed for %s: %s", instanceInfo.getInstanceId(), e.getMessage());
            return MySQLHealthStatus.builder()
                    .instanceId(instanceInfo.getInstanceId())
                    .tenantName(instanceInfo.getTenantName())
                    .host(instanceInfo.getHost())
                    .port(instanceInfo.getPort())
                    .database(instanceInfo.getDatabase())
                    .status("DOWN")
                    .error("Connection failed: " + e.getMessage())
                    .build();
        }
    }

    private Connection createConnection(MySQLInstanceInfo instanceInfo) throws SQLException {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000",
                instanceInfo.getHost(),
                instanceInfo.getPort(),
                instanceInfo.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", instanceInfo.getUsername());
        if (instanceInfo.getPassword() != null && !instanceInfo.getPassword().isEmpty()) {
            props.setProperty("password", instanceInfo.getPassword());
        }

        return DriverManager.getConnection(jdbcUrl, props);
    }

    private MySQLHealthStatus checkHealth(Connection conn, MySQLInstanceInfo instanceInfo) {
        try {
            String version = null;
            long uptime = 0;
            int activeConnections = 0;
            int maxConnections = 0;
            String role = "standalone";

            // Get version and uptime
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name IN ('Uptime', 'Threads_connected')")) {

                while (rs.next()) {
                    String varName = rs.getString("Variable_name");
                    String value = rs.getString("Value");

                    switch (varName) {
                        case "Uptime":
                            uptime = Long.parseLong(value);
                            break;
                        case "Threads_connected":
                            activeConnections = Integer.parseInt(value);
                            break;
                    }
                }
            }

            // Get MySQL version
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT VERSION() as version")) {
                if (rs.next()) {
                    version = rs.getString("version");
                }
            }

            // Get max connections
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'max_connections'")) {
                if (rs.next()) {
                    maxConnections = Integer.parseInt(rs.getString("Value"));
                }
            }

            // Check replication status (for slaves)
            MySQLReplicationStatus replication = null;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW SLAVE STATUS")) {

                if (rs.next()) {
                    role = "slave";
                    replication = MySQLReplicationStatus.builder()
                            .slaveIOState(rs.getString("Slave_IO_State"))
                            .masterHost(rs.getString("Master_Host"))
                            .masterPort(rs.getInt("Master_Port"))
                            .masterUser(rs.getString("Master_User"))
                            .slaveIORunning("Yes".equalsIgnoreCase(rs.getString("Slave_IO_Running")))
                            .slaveSQLRunning("Yes".equalsIgnoreCase(rs.getString("Slave_SQL_Running")))
                            .secondsBehindMaster(rs.getLong("Seconds_Behind_Master"))
                            .lastError(rs.getString("Last_Error"))
                            .replicationHealthy(
                                    "Yes".equalsIgnoreCase(rs.getString("Slave_IO_Running")) &&
                                    "Yes".equalsIgnoreCase(rs.getString("Slave_SQL_Running")) &&
                                    rs.getLong("Seconds_Behind_Master") < 60
                            )
                            .build();
                }
            } catch (SQLException e) {
                // Not a slave or no permission
            }

            // Check master status
            MySQLMasterStatus masterStatus = null;
            if (role.equals("standalone")) {
                List<MySQLMasterStatus.SlaveInfo> connectedSlaves = new ArrayList<>();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SHOW SLAVE HOSTS")) {

                    while (rs.next()) {
                        role = "master";
                        MySQLMasterStatus.SlaveInfo slaveInfo = MySQLMasterStatus.SlaveInfo.builder()
                                .serverId(rs.getInt("Server_id"))
                                .host(rs.getString("Host"))
                                .port(rs.getInt("Port"))
                                .uuid(rs.getString("Slave_UUID"))
                                .build();
                        connectedSlaves.add(slaveInfo);
                    }
                } catch (SQLException e) {
                    // No permission or error
                }

                if (role.equals("master") || role.equals("standalone")) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {

                        if (rs.next()) {
                            String binlogFile = rs.getString("File");
                            long binlogPosition = rs.getLong("Position");

                            masterStatus = MySQLMasterStatus.builder()
                                    .binlogFile(binlogFile)
                                    .binlogPosition(binlogPosition)
                                    .connectedSlaves(connectedSlaves)
                                    .slaveCount(connectedSlaves.size())
                                    .build();

                            if (connectedSlaves.isEmpty()) {
                                role = "standalone";
                            } else {
                                role = "master";
                            }
                        }
                    } catch (SQLException e) {
                        // No permission or binlog not enabled
                    }
                }
            }

            return MySQLHealthStatus.builder()
                    .instanceId(instanceInfo.getInstanceId())
                    .tenantName(instanceInfo.getTenantName())
                    .host(instanceInfo.getHost())
                    .port(instanceInfo.getPort())
                    .database(instanceInfo.getDatabase())
                    .role(role)
                    .status("UP")
                    .version(version)
                    .uptimeSeconds(uptime)
                    .activeConnections(activeConnections)
                    .maxConnections(maxConnections)
                    .replication(replication)
                    .masterStatus(masterStatus)
                    .build();

        } catch (SQLException e) {
            return MySQLHealthStatus.builder()
                    .instanceId(instanceInfo.getInstanceId())
                    .tenantName(instanceInfo.getTenantName())
                    .host(instanceInfo.getHost())
                    .port(instanceInfo.getPort())
                    .database(instanceInfo.getDatabase())
                    .status("DOWN")
                    .error("Health check failed: " + e.getMessage())
                    .build();
        }
    }
}
