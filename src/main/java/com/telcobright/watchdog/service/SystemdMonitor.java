package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Monitor and manage systemd services.
 */
@ApplicationScoped
public class SystemdMonitor {

    private static final Logger LOG = Logger.getLogger(SystemdMonitor.class);

    @Inject
    CommandExecutor executor;

    /**
     * Check if a systemd service is active (running).
     */
    public boolean isServiceActive(String serviceName) {
        CommandResult result = executor.execute("systemctl is-active " + serviceName);
        return result.isSuccess() && "active".equals(result.stdout().trim());
    }

    /**
     * Check if a systemd service is enabled (starts on boot).
     */
    public boolean isServiceEnabled(String serviceName) {
        CommandResult result = executor.execute("systemctl is-enabled " + serviceName);
        return result.isSuccess() && "enabled".equals(result.stdout().trim());
    }

    /**
     * Get service status details.
     */
    public ServiceStatus getServiceStatus(String serviceName) {
        boolean active = isServiceActive(serviceName);
        boolean enabled = isServiceEnabled(serviceName);

        String state = "unknown";
        String description = "";

        CommandResult result = executor.execute("systemctl show " + serviceName +
            " --property=ActiveState,SubState,Description --no-pager");

        if (result.isSuccess()) {
            for (String line : result.stdout().split("\n")) {
                if (line.startsWith("ActiveState=")) {
                    state = line.substring("ActiveState=".length());
                } else if (line.startsWith("Description=")) {
                    description = line.substring("Description=".length());
                }
            }
        }

        return new ServiceStatus(serviceName, active, enabled, state, description);
    }

    /**
     * Start a systemd service.
     */
    public CommandResult startService(String serviceName) {
        LOG.infof("Starting service: %s", serviceName);
        return executor.executeSudo("systemctl start " + serviceName);
    }

    /**
     * Stop a systemd service.
     */
    public CommandResult stopService(String serviceName) {
        LOG.infof("Stopping service: %s", serviceName);
        return executor.executeSudo("systemctl stop " + serviceName);
    }

    /**
     * Restart a systemd service.
     */
    public CommandResult restartService(String serviceName) {
        LOG.infof("Restarting service: %s", serviceName);
        return executor.executeSudo("systemctl restart " + serviceName);
    }

    /**
     * Enable a systemd service (start on boot).
     */
    public CommandResult enableService(String serviceName) {
        LOG.infof("Enabling service: %s", serviceName);
        return executor.executeSudo("systemctl enable " + serviceName);
    }

    /**
     * Check and start service if not running.
     * Returns true if service is now running.
     */
    public boolean ensureServiceRunning(String serviceName) {
        if (isServiceActive(serviceName)) {
            return true;
        }

        LOG.infof("Service %s not running, starting...", serviceName);
        CommandResult result = startService(serviceName);

        if (!result.isSuccess()) {
            LOG.errorf("Failed to start service %s: %s", serviceName, result.stderr());
            return false;
        }

        // Verify it started
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return isServiceActive(serviceName);
    }

    public record ServiceStatus(
            String name,
            boolean active,
            boolean enabled,
            String state,
            String description
    ) {}
}
