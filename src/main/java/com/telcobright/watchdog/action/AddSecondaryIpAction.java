package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.service.CommandExecutor;
import com.telcobright.watchdog.service.NetworkMonitor;
import org.jboss.logging.Logger;

/**
 * Action to add a secondary IP address to a network interface.
 * Used for ensuring Kafka overlay IPs are present on lxdbr0.
 */
public class AddSecondaryIpAction implements Action {

    private static final Logger LOG = Logger.getLogger(AddSecondaryIpAction.class);

    private final NetworkMonitor networkMonitor;
    private final String interfaceName;
    private final String ipAddress;

    public AddSecondaryIpAction(NetworkMonitor networkMonitor, String interfaceName, String ipAddress) {
        this.networkMonitor = networkMonitor;
        this.interfaceName = interfaceName;
        this.ipAddress = ipAddress;
    }

    @Override
    public CommandResult execute() {
        if (!isNeeded()) {
            LOG.debugf("IP %s already exists on %s, skipping", ipAddress, interfaceName);
            return CommandResult.success("noop", "IP already exists", java.time.Duration.ZERO);
        }

        LOG.infof("Adding IP %s to interface %s", ipAddress, interfaceName);
        return networkMonitor.addIpAddress(interfaceName, ipAddress);
    }

    @Override
    public boolean isNeeded() {
        return !networkMonitor.hasIpAddress(interfaceName, ipAddress.split("/")[0]);
    }

    @Override
    public String getDescription() {
        return "Add IP " + ipAddress + " to " + interfaceName;
    }
}
