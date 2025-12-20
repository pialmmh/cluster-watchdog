package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.service.WireGuardMonitor;
import org.jboss.logging.Logger;

/**
 * Action to start a WireGuard interface.
 */
public class StartWireGuardAction implements Action {

    private static final Logger LOG = Logger.getLogger(StartWireGuardAction.class);

    private final WireGuardMonitor wireGuardMonitor;
    private final String interfaceName;

    public StartWireGuardAction(WireGuardMonitor wireGuardMonitor, String interfaceName) {
        this.wireGuardMonitor = wireGuardMonitor;
        this.interfaceName = interfaceName;
    }

    @Override
    public CommandResult execute() {
        if (!isNeeded()) {
            LOG.debugf("WireGuard interface %s already up, skipping", interfaceName);
            return CommandResult.success("noop", "Interface already up", java.time.Duration.ZERO);
        }

        LOG.infof("Starting WireGuard interface: %s", interfaceName);
        return wireGuardMonitor.startInterface(interfaceName);
    }

    @Override
    public boolean isNeeded() {
        return !wireGuardMonitor.isInterfaceUp(interfaceName);
    }

    @Override
    public String getDescription() {
        return "Start WireGuard interface: " + interfaceName;
    }
}
