package com.telcobright.watchdog.scheduler;

import com.telcobright.watchdog.action.AddSecondaryIpAction;
import com.telcobright.watchdog.action.HoldSnapRefreshAction;
import com.telcobright.watchdog.action.StartWireGuardAction;
import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.service.*;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Scheduled monitoring that fixes LOCAL node issues only.
 * REST APIs expose cluster-wide info for remote queries.
 */
@ApplicationScoped
public class ClusterMonitorScheduler {

    private static final Logger LOG = Logger.getLogger(ClusterMonitorScheduler.class);

    @Inject
    ClusterConfig config;

    @Inject
    NetworkMonitor networkMonitor;

    @Inject
    WireGuardMonitor wireGuardMonitor;

    @Inject
    DockerMonitor dockerMonitor;

    @Inject
    LxcMonitor lxcMonitor;

    @Inject
    KafkaMonitor kafkaMonitor;

    @Inject
    SnapManager snapManager;

    @Inject
    BgpMonitor bgpMonitor;

    @Inject
    SystemdMonitor systemdMonitor;

    private volatile Instant lastCheck;
    private volatile List<String> lastAlerts = new ArrayList<>();
    private volatile com.telcobright.watchdog.model.BgpRoutingStatus lastBgpRoutingStatus;

    @Scheduled(every = "${watchdog.check-interval:60s}")
    void monitorLocal() {
        LOG.infof("=== Local health check started - Node %d ===", config.local().nodeId());
        lastCheck = Instant.now();
        lastAlerts.clear();

        // Fix LOCAL issues only
        checkKafkaIpService();
        checkAndFixLocalNetworkIp();
        checkLocalWireGuard();
        checkCriticalSystemdServices();
        checkLocalDockerContainers();
        checkLocalLxcContainers();
        checkSnapRefreshHold();

        if (!lastAlerts.isEmpty()) {
            LOG.warnf("=== Local check completed with %d alerts ===", lastAlerts.size());
            for (String alert : lastAlerts) {
                LOG.warnf("  ALERT: %s", alert);
            }
        } else {
            LOG.infof("=== Local check completed - all healthy ===");
        }
    }

    @Scheduled(every = "${watchdog.kafka-check-interval:120s}")
    void monitorKafka() {
        LOG.debug("Running Kafka cluster check");

        String bootstrap = config.kafka().bootstrapServers();
        var status = kafkaMonitor.checkCluster(bootstrap);

        if (status.availableBrokers() < status.totalBrokers()) {
            String alert = String.format("Kafka: only %d/%d brokers available",
                    status.availableBrokers(), status.totalBrokers());
            LOG.warn(alert);
            lastAlerts.add(alert);
        }

        if (!status.controllerId()) {
            LOG.warn("Kafka: no controller elected");
            lastAlerts.add("Kafka: no controller elected");
        }
    }

    @Scheduled(every = "${watchdog.bgp-check-interval:300s}")
    void monitorBgp() {
        LOG.debug("Running BGP routing check");

        String frrContainer = config.local().frrContainer().orElse(null);
        var routingStatus = bgpMonitor.getRoutingStatus(frrContainer);

        if (!routingStatus.frrRunning()) {
            LOG.warn("[BGP] FRR is not running");
            lastAlerts.add("FRR not running");
            return;
        }

        // Check peer status
        if (routingStatus.establishedPeers() < routingStatus.totalPeers()) {
            String alert = String.format("BGP: only %d/%d peers established",
                    routingStatus.establishedPeers(), routingStatus.totalPeers());
            LOG.warn("[BGP] " + alert);
            lastAlerts.add(alert);
        }

        // Check routing health - alert on missing routes
        switch (routingStatus.health()) {
            case HEALTHY:
                LOG.infof("[BGP] Routing healthy: %s", routingStatus.message());
                break;
            case DEGRADED:
                LOG.warnf("[BGP] Routing degraded: %s", routingStatus.message());
                lastAlerts.add("BGP routing degraded: " + routingStatus.message());
                logMissingRoutes(routingStatus);
                break;
            case UNHEALTHY:
                LOG.errorf("[BGP] Routing unhealthy: %s", routingStatus.message());
                lastAlerts.add("BGP routing unhealthy: " + routingStatus.message());
                logMissingRoutes(routingStatus);
                break;
            default:
                break;
        }

        // Store for API queries
        lastBgpRoutingStatus = routingStatus;
    }

    private void logMissingRoutes(com.telcobright.watchdog.model.BgpRoutingStatus status) {
        for (var check : status.missingRoutes()) {
            LOG.warnf("[BGP] Missing route: %s (type: %s)", check.subnet(), check.type());
        }
    }

    private void checkAndFixLocalNetworkIp() {
        // Fix LOCAL bridge secondary IP
        String bridgeName = config.local().bridge();
        String overlayIp = config.local().overlayIp().orElse(null);

        if (overlayIp != null && !overlayIp.isEmpty()) {
            LOG.debugf("[NETWORK] Checking bridge %s for IP %s", bridgeName, overlayIp);
            var action = new AddSecondaryIpAction(networkMonitor, bridgeName, overlayIp);
            if (action.isNeeded()) {
                LOG.warnf("[NETWORK] Secondary IP %s missing on %s, adding...", overlayIp, bridgeName);
                var result = action.execute();
                if (result.isSuccess()) {
                    LOG.infof("[NETWORK] Secondary IP %s added to %s successfully", overlayIp, bridgeName);
                } else {
                    String alert = "Failed to add secondary IP " + overlayIp + ": " + result.stderr();
                    LOG.errorf("[NETWORK] %s", alert);
                    lastAlerts.add(alert);
                }
            } else {
                LOG.debugf("[NETWORK] Secondary IP %s already present on %s", overlayIp, bridgeName);
            }
        }
    }

    private void checkLocalWireGuard() {
        String wgInterface = config.local().wireGuardInterface();
        if (wgInterface == null || wgInterface.isEmpty()) {
            return;
        }

        LOG.debugf("[WIREGUARD] Checking interface: %s", wgInterface);
        var action = new StartWireGuardAction(wireGuardMonitor, wgInterface);
        if (action.isNeeded()) {
            LOG.warnf("[WIREGUARD] Interface %s is down, starting...", wgInterface);
            var result = action.execute();
            if (result.isSuccess()) {
                LOG.infof("[WIREGUARD] Interface %s started successfully", wgInterface);
            } else {
                String alert = "Failed to start WireGuard " + wgInterface + ": " + result.stderr();
                LOG.errorf("[WIREGUARD] %s", alert);
                lastAlerts.add(alert);
            }
        } else {
            LOG.debugf("[WIREGUARD] Interface %s is up", wgInterface);
        }

        // Check peer connectivity
        var status = wireGuardMonitor.checkInterface(wgInterface);
        if ("DEGRADED".equals(status.status().name())) {
            LOG.warnf("[WIREGUARD] %s: %s", wgInterface, status.message());
            lastAlerts.add("WireGuard " + wgInterface + ": " + status.message());
        } else {
            LOG.debugf("[WIREGUARD] %s peer status: %s", wgInterface, status.message());
        }
    }

    private void checkLocalDockerContainers() {
        LOG.debug("[DOCKER] Checking Docker daemon and containers");
        if (!dockerMonitor.isDockerRunning()) {
            LOG.error("[DOCKER] Docker daemon not running");
            lastAlerts.add("Docker daemon not running");
            return;
        }
        LOG.debug("[DOCKER] Docker daemon is running");

        var containers = dockerMonitor.listContainers();
        int running = 0, stopped = 0;
        for (var container : containers) {
            if (container.state() == com.telcobright.watchdog.model.ContainerStatus.State.RUNNING) {
                running++;
            } else {
                stopped++;
                if (isCriticalContainer(container.name())) {
                    LOG.warnf("[DOCKER] Critical container stopped: %s (state: %s)", container.name(), container.state());
                    lastAlerts.add("Critical Docker container stopped: " + container.name());
                }
            }
        }
        LOG.debugf("[DOCKER] Containers: %d running, %d stopped", running, stopped);
    }

    private void checkLocalLxcContainers() {
        LOG.debug("[LXC] Checking LXD daemon and containers");
        if (!lxcMonitor.isLxdRunning()) {
            LOG.error("[LXC] LXD daemon not running");
            lastAlerts.add("LXD daemon not running");
            return;
        }
        LOG.debug("[LXC] LXD daemon is running");

        var containers = lxcMonitor.listContainers();
        int running = 0, stopped = 0;
        for (var container : containers) {
            if (container.state() == com.telcobright.watchdog.model.ContainerStatus.State.RUNNING) {
                running++;
            } else {
                stopped++;
                if (isCriticalContainer(container.name())) {
                    LOG.warnf("[LXC] Critical container stopped: %s (state: %s)", container.name(), container.state());
                    lastAlerts.add("Critical LXC container stopped: " + container.name());
                }
            }
        }
        LOG.debugf("[LXC] Containers: %d running, %d stopped", running, stopped);
    }

    private void checkSnapRefreshHold() {
        if (config.snap().holdLxdRefresh()) {
            var action = new HoldSnapRefreshAction(snapManager, "lxd");
            if (action.isNeeded()) {
                LOG.info("[SNAP] LXD snap refresh not held, applying hold...");
                var result = action.execute();
                if (result.isSuccess()) {
                    LOG.info("[SNAP] LXD snap refresh hold applied successfully");
                } else {
                    LOG.warnf("[SNAP] Failed to hold LXD refresh: %s", result.stderr());
                }
            } else {
                LOG.debug("[SNAP] LXD snap refresh already held");
            }
        }
    }

    private void checkKafkaIpService() {
        String serviceName = config.local().kafkaIpService();
        LOG.debugf("[SYSTEMD] Checking Kafka IP service: %s", serviceName);

        if (!systemdMonitor.isServiceActive(serviceName)) {
            LOG.warnf("[SYSTEMD] Service %s not running, starting...", serviceName);
            var result = systemdMonitor.startService(serviceName);
            if (result.isSuccess()) {
                LOG.infof("[SYSTEMD] Service %s started successfully", serviceName);
            } else {
                String alert = "Failed to start " + serviceName + ": " + result.stderr();
                LOG.errorf("[SYSTEMD] %s", alert);
                lastAlerts.add(alert);
            }
        } else {
            LOG.debugf("[SYSTEMD] Service %s is running", serviceName);
        }

        // Ensure it's enabled for boot
        if (!systemdMonitor.isServiceEnabled(serviceName)) {
            LOG.warnf("[SYSTEMD] Service %s not enabled, enabling...", serviceName);
            systemdMonitor.enableService(serviceName);
        }
    }

    private void checkCriticalSystemdServices() {
        var criticalServices = config.local().criticalServices();
        if (criticalServices.isEmpty()) {
            return;
        }

        for (String serviceName : criticalServices.get()) {
            LOG.debugf("[SYSTEMD] Checking critical service: %s", serviceName);
            if (!systemdMonitor.isServiceActive(serviceName)) {
                String alert = "Critical service not running: " + serviceName;
                LOG.warnf("[SYSTEMD] %s", alert);
                lastAlerts.add(alert);

                // Try to start it
                LOG.infof("[SYSTEMD] Attempting to start %s...", serviceName);
                var result = systemdMonitor.startService(serviceName);
                if (result.isSuccess()) {
                    LOG.infof("[SYSTEMD] Service %s started successfully", serviceName);
                } else {
                    LOG.errorf("[SYSTEMD] Failed to start %s: %s", serviceName, result.stderr());
                }
            }
        }
    }

    private boolean isCriticalContainer(String name) {
        String lower = name.toLowerCase();
        return lower.contains("kafka") ||
                lower.contains("mysql") ||
                lower.contains("frr") ||
                lower.contains("zookeeper");
    }

    public Instant getLastCheck() {
        return lastCheck;
    }

    public List<String> getLastAlerts() {
        return new ArrayList<>(lastAlerts);
    }

    public int getLocalNodeId() {
        return config.local().nodeId();
    }

    public com.telcobright.watchdog.model.BgpRoutingStatus getLastBgpRoutingStatus() {
        return lastBgpRoutingStatus;
    }
}
