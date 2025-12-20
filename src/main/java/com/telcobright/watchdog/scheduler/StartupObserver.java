package com.telcobright.watchdog.scheduler;

import com.telcobright.watchdog.action.AddSecondaryIpAction;
import com.telcobright.watchdog.action.HoldSnapRefreshAction;
import com.telcobright.watchdog.action.StartWireGuardAction;
import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.service.NetworkMonitor;
import com.telcobright.watchdog.service.SnapManager;
import com.telcobright.watchdog.service.WireGuardMonitor;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Startup actions - fixes LOCAL node issues only.
 */
@ApplicationScoped
public class StartupObserver {

    private static final Logger LOG = Logger.getLogger(StartupObserver.class);

    @Inject
    ClusterConfig config;

    @Inject
    SnapManager snapManager;

    @Inject
    NetworkMonitor networkMonitor;

    @Inject
    WireGuardMonitor wireGuardMonitor;

    void onStart(@Observes StartupEvent event) {
        LOG.infof("Cluster Watchdog starting - Node %d (%s)",
                config.local().nodeId(), config.tenant());

        // Hold LXD snap refresh to prevent auto-updates
        if (config.snap().holdLxdRefresh()) {
            holdLxdRefresh();
        }

        // Ensure LOCAL WireGuard is running
        ensureLocalWireGuardUp();

        // Ensure LOCAL secondary IP is configured
        ensureLocalSecondaryIp();

        LOG.infof("Cluster Watchdog startup completed - Node %d", config.local().nodeId());
    }

    private void holdLxdRefresh() {
        LOG.info("Checking LXD snap refresh hold status...");

        var action = new HoldSnapRefreshAction(snapManager, "lxd");
        if (action.isNeeded()) {
            LOG.info("Holding LXD snap refresh to prevent automatic updates");
            var result = action.execute();
            if (result.isSuccess()) {
                LOG.info("LXD snap refresh held successfully");
            } else {
                LOG.errorf("Failed to hold LXD snap refresh: %s", result.stderr());
            }
        } else {
            LOG.info("LXD snap refresh already held");
        }
    }

    private void ensureLocalWireGuardUp() {
        String wgInterface = config.local().wireGuardInterface();
        if (wgInterface == null || wgInterface.isEmpty()) {
            LOG.debug("No WireGuard interface configured");
            return;
        }

        LOG.infof("Checking WireGuard interface: %s", wgInterface);

        var action = new StartWireGuardAction(wireGuardMonitor, wgInterface);
        if (action.isNeeded()) {
            LOG.infof("Starting WireGuard interface: %s", wgInterface);
            var result = action.execute();
            if (result.isSuccess()) {
                LOG.info("WireGuard interface started successfully");
            } else {
                LOG.errorf("Failed to start WireGuard: %s", result.stderr());
            }
        } else {
            LOG.info("WireGuard interface already up");
        }
    }

    private void ensureLocalSecondaryIp() {
        String bridgeName = config.local().bridge();
        String overlayIp = config.local().overlayIp().orElse(null);

        if (overlayIp == null || overlayIp.isEmpty()) {
            LOG.debug("No overlay IP configured");
            return;
        }

        LOG.infof("Checking secondary IP %s on %s", overlayIp, bridgeName);

        var action = new AddSecondaryIpAction(networkMonitor, bridgeName, overlayIp);
        if (action.isNeeded()) {
            LOG.infof("Adding secondary IP %s to %s", overlayIp, bridgeName);
            var result = action.execute();
            if (result.isSuccess()) {
                LOG.info("Secondary IP added successfully");
            } else {
                LOG.errorf("Failed to add secondary IP: %s", result.stderr());
            }
        } else {
            LOG.info("Secondary IP already configured");
        }
    }
}
