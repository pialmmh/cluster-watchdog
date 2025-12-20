package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.model.ContainerStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class LxcMonitor {

    private static final Logger LOG = Logger.getLogger(LxcMonitor.class);

    @Inject
    CommandExecutor executor;

    public List<ContainerStatus> listContainers() {
        CommandResult result = executor.execute("lxc list --format csv -c ns");

        if (!result.isSuccess()) {
            LOG.warnf("Failed to list LXC containers: %s", result.stderr());
            return List.of();
        }

        return parseContainerList(result.stdout());
    }

    public ContainerStatus checkContainer(String containerName) {
        CommandResult result = executor.execute("lxc info " + containerName);

        if (!result.isSuccess()) {
            return ContainerStatus.notFound(containerName, ContainerStatus.ContainerType.LXC);
        }

        return parseContainerInfo(containerName, result.stdout());
    }

    public boolean isContainerRunning(String containerName) {
        CommandResult result = executor.execute("lxc info " + containerName + " | grep -i 'status:'");
        return result.isSuccess() && result.stdout().toLowerCase().contains("running");
    }

    public CommandResult startContainer(String containerName) {
        LOG.infof("Starting LXC container: %s", containerName);
        return executor.execute("lxc start " + containerName);
    }

    public CommandResult stopContainer(String containerName) {
        LOG.infof("Stopping LXC container: %s", containerName);
        return executor.execute("lxc stop " + containerName);
    }

    public CommandResult restartContainer(String containerName) {
        LOG.infof("Restarting LXC container: %s", containerName);
        return executor.execute("lxc restart " + containerName);
    }

    public String getContainerIp(String containerName) {
        CommandResult result = executor.execute(
                "lxc list " + containerName + " --format csv -c 4");
        if (result.isSuccess() && !result.stdout().trim().isEmpty()) {
            String ipLine = result.stdout().trim();
            if (ipLine.contains("(")) {
                return ipLine.substring(0, ipLine.indexOf("(")).trim();
            }
            return ipLine.split(",")[0].trim();
        }
        return null;
    }

    public boolean isLxdRunning() {
        CommandResult result = executor.execute("lxc version");
        return result.isSuccess();
    }

    public CommandResult execInContainer(String containerName, String command) {
        return executor.execute("lxc exec " + containerName + " -- " + command);
    }

    private List<ContainerStatus> parseContainerList(String output) {
        List<ContainerStatus> containers = new ArrayList<>();
        for (String line : output.split("\n")) {
            if (line.trim().isEmpty()) continue;

            String[] parts = line.split(",");
            if (parts.length >= 2) {
                String name = parts[0].trim();
                String status = parts[1].trim();

                ContainerStatus.State state = switch (status.toUpperCase()) {
                    case "RUNNING" -> ContainerStatus.State.RUNNING;
                    case "STOPPED" -> ContainerStatus.State.STOPPED;
                    case "FROZEN" -> ContainerStatus.State.PAUSED;
                    default -> ContainerStatus.State.UNKNOWN;
                };

                containers.add(new ContainerStatus(
                        name, name, ContainerStatus.ContainerType.LXC, state, status, "lxc"));
            }
        }
        return containers;
    }

    private ContainerStatus parseContainerInfo(String name, String output) {
        String status = "unknown";
        for (String line : output.split("\n")) {
            if (line.toLowerCase().contains("status:")) {
                status = line.split(":")[1].trim();
                break;
            }
        }

        ContainerStatus.State state = switch (status.toUpperCase()) {
            case "RUNNING" -> ContainerStatus.State.RUNNING;
            case "STOPPED" -> ContainerStatus.State.STOPPED;
            case "FROZEN" -> ContainerStatus.State.PAUSED;
            default -> ContainerStatus.State.UNKNOWN;
        };

        return new ContainerStatus(name, name, ContainerStatus.ContainerType.LXC, state, status, "lxc");
    }
}
