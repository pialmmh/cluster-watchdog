package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.service.CommandExecutor;
import org.jboss.logging.Logger;

/**
 * Action to restart a systemd service.
 */
public class RestartServiceAction implements Action {

    private static final Logger LOG = Logger.getLogger(RestartServiceAction.class);

    private final CommandExecutor executor;
    private final String serviceName;

    public RestartServiceAction(CommandExecutor executor, String serviceName) {
        this.executor = executor;
        this.serviceName = serviceName;
    }

    @Override
    public CommandResult execute() {
        LOG.infof("Restarting service: %s", serviceName);
        return executor.executeSudo("systemctl restart " + serviceName);
    }

    @Override
    public boolean isNeeded() {
        CommandResult result = executor.execute("systemctl is-active " + serviceName);
        return !result.isSuccess() || !result.stdout().trim().equals("active");
    }

    @Override
    public String getDescription() {
        return "Restart systemd service: " + serviceName;
    }

    public boolean isActive() {
        CommandResult result = executor.execute("systemctl is-active " + serviceName);
        return result.isSuccess() && result.stdout().trim().equals("active");
    }

    public CommandResult start() {
        LOG.infof("Starting service: %s", serviceName);
        return executor.executeSudo("systemctl start " + serviceName);
    }

    public CommandResult stop() {
        LOG.infof("Stopping service: %s", serviceName);
        return executor.executeSudo("systemctl stop " + serviceName);
    }

    public CommandResult enable() {
        LOG.infof("Enabling service: %s", serviceName);
        return executor.executeSudo("systemctl enable " + serviceName);
    }
}
