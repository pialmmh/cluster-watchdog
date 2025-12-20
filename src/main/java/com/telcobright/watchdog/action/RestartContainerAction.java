package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.model.ContainerStatus;
import com.telcobright.watchdog.service.DockerMonitor;
import com.telcobright.watchdog.service.LxcMonitor;
import org.jboss.logging.Logger;

/**
 * Action to restart a container (Docker or LXC).
 */
public class RestartContainerAction implements Action {

    private static final Logger LOG = Logger.getLogger(RestartContainerAction.class);

    private final DockerMonitor dockerMonitor;
    private final LxcMonitor lxcMonitor;
    private final String containerName;
    private final ContainerStatus.ContainerType containerType;

    public RestartContainerAction(DockerMonitor dockerMonitor, LxcMonitor lxcMonitor,
                                   String containerName, ContainerStatus.ContainerType containerType) {
        this.dockerMonitor = dockerMonitor;
        this.lxcMonitor = lxcMonitor;
        this.containerName = containerName;
        this.containerType = containerType;
    }

    @Override
    public CommandResult execute() {
        LOG.infof("Restarting %s container: %s", containerType, containerName);

        return switch (containerType) {
            case DOCKER -> dockerMonitor.restartContainer(containerName);
            case LXC -> lxcMonitor.restartContainer(containerName);
        };
    }

    @Override
    public boolean isNeeded() {
        return switch (containerType) {
            case DOCKER -> !dockerMonitor.isContainerRunning(containerName);
            case LXC -> !lxcMonitor.isContainerRunning(containerName);
        };
    }

    @Override
    public String getDescription() {
        return "Restart " + containerType + " container: " + containerName;
    }
}
