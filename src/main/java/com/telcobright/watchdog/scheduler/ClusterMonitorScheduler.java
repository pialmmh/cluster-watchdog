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

    private volatile Instant lastCheck;
    private volatile List<String> lastAlerts = new ArrayList<>();

    @Scheduled(every = "${watchdog.check-interval:60s}")
    void monitorLocal() {
        LOG.debugf("Running local check for node %d", config.local().nodeId());
        lastCheck = Instant.now();
        lastAlerts.clear();

        // Fix LOCAL issues only
        checkAndFixLocalNetworkIp();
        checkLocalWireGuard();
        checkLocalDockerContainers();
        checkLocalLxcContainers();
        checkSnapRefreshHold();

        if (!lastAlerts.isEmpty()) {
            LOG.warnf("Local check completed with %d alerts", lastAlerts.size());
        } else {
            LOG.debug("Local check completed - all healthy");
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
        LOG.debug("Running BGP check");

        String frrContainer = config.local().frrContainer().orElse(null);
        var status = bgpMonitor.getStatus(frrContainer);

        if (!status.frrRunning()) {
            LOG.warn("FRR is not running");
            lastAlerts.add("FRR not running");
            return;
        }

        if (status.establishedPeers() < status.totalPeers()) {
            String alert = String.format("BGP: only %d/%d peers established",
                    status.establishedPeers(), status.totalPeers());
            LOG.warn(alert);
            lastAlerts.add(alert);
        }
    }

    private void checkAndFixLocalNetworkIp() {
        // Fix LOCAL bridge secondary IP
        String bridgeName = config.local().bridge();
        String overlayIp = config.local().overlayIp().orElse(null);

        if (overlayIp != null && !overlayIp.isEmpty()) {
            var action = new AddSecondaryIpAction(networkMonitor, bridgeName, overlayIp);
            if (action.isNeeded()) {
                LOG.infof("Secondary IP %s missing on %s, adding...", overlayIp, bridgeName);
                var result = action.execute();
                if (result.isSuccess()) {
                    LOG.info("Secondary IP added successfully");
                } else {
                    String alert = "Failed to add secondary IP: " + result.stderr();
                    LOG.error(alert);
                    lastAlerts.add(alert);
                }
            }
        }
    }

    private void checkLocalWireGuard() {
        String wgInterface = config.local().wireGuardInterface();
        if (wgInterface == null || wgInterface.isEmpty()) {
            return;
        }

        var action = new StartWireGuardAction(wireGuardMonitor, wgInterface);
        if (action.isNeeded()) {
            LOG.infof("WireGuard interface %s is down, starting...", wgInterface);
            var result = action.execute();
            if (!result.isSuccess()) {
                String alert = "Failed to start WireGuard: " + result.stderr();
                LOG.error(alert);
                lastAlerts.add(alert);
            }
        }

        // Check peer connectivity
        var status = wireGuardMonitor.checkInterface(wgInterface);
        if ("DEGRADED".equals(status.status().name())) {
            lastAlerts.add("WireGuard: " + status.message());
        }
    }

    private void checkLocalDockerContainers() {
        if (!dockerMonitor.isDockerRunning()) {
            lastAlerts.add("Docker daemon not running");
            return;
        }

        var containers = dockerMonitor.listContainers();
        for (var container : containers) {
            if (container.state() != com.telcobright.watchdog.model.ContainerStatus.State.RUNNING) {
                if (isCriticalContainer(container.name())) {
                    lastAlerts.add("Critical container stopped: " + container.name());
                }
            }
        }
    }

    private void checkLocalLxcContainers() {
        if (!lxcMonitor.isLxdRunning()) {
            lastAlerts.add("LXD daemon not running");
            return;
        }

        var containers = lxcMonitor.listContainers();
        for (var container : containers) {
            if (container.state() != com.telcobright.watchdog.model.ContainerStatus.State.RUNNING) {
                if (isCriticalContainer(container.name())) {
                    lastAlerts.add("Critical LXC container stopped: " + container.name());
                }
            }
        }
    }

    private void checkSnapRefreshHold() {
        if (config.snap().holdLxdRefresh()) {
            var action = new HoldSnapRefreshAction(snapManager, "lxd");
            if (action.isNeeded()) {
                LOG.info("LXD snap refresh not held, applying hold...");
                var result = action.execute();
                if (!result.isSuccess()) {
                    LOG.warn("Failed to hold LXD refresh: " + result.stderr());
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
}
