package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.model.ContainerStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class DockerMonitor {

    private static final Logger LOG = Logger.getLogger(DockerMonitor.class);

    @Inject
    CommandExecutor executor;

    public List<ContainerStatus> listContainers() {
        CommandResult result = executor.execute("docker ps -a --format '{{.ID}}|{{.Names}}|{{.Status}}|{{.Image}}'");

        if (!result.isSuccess()) {
            LOG.warnf("Failed to list docker containers: %s", result.stderr());
            return List.of();
        }

        return parseContainerList(result.stdout());
    }

    public ContainerStatus checkContainer(String containerName) {
        CommandResult result = executor.execute(
                "docker inspect --format '{{.Id}}|{{.Name}}|{{.State.Status}}|{{.Config.Image}}' " + containerName);

        if (!result.isSuccess()) {
            return ContainerStatus.notFound(containerName, ContainerStatus.ContainerType.DOCKER);
        }

        return parseContainerInspect(result.stdout());
    }

    public boolean isContainerRunning(String containerName) {
        CommandResult result = executor.execute(
                "docker inspect --format '{{.State.Running}}' " + containerName);
        return result.isSuccess() && result.stdout().trim().equals("true");
    }

    public CommandResult startContainer(String containerName) {
        LOG.infof("Starting docker container: %s", containerName);
        return executor.execute("docker start " + containerName);
    }

    public CommandResult restartContainer(String containerName) {
        LOG.infof("Restarting docker container: %s", containerName);
        return executor.execute("docker restart " + containerName);
    }

    public CommandResult stopContainer(String containerName) {
        LOG.infof("Stopping docker container: %s", containerName);
        return executor.execute("docker stop " + containerName);
    }

    public CommandResult composeUp(String composeDir) {
        LOG.infof("Starting docker-compose in: %s", composeDir);
        return executor.execute("docker compose -f " + composeDir + "/docker-compose.yml up -d");
    }

    public CommandResult composeDown(String composeDir) {
        LOG.infof("Stopping docker-compose in: %s", composeDir);
        return executor.execute("docker compose -f " + composeDir + "/docker-compose.yml down");
    }

    public boolean isDockerRunning() {
        CommandResult result = executor.execute("docker info");
        return result.isSuccess();
    }

    private List<ContainerStatus> parseContainerList(String output) {
        List<ContainerStatus> containers = new ArrayList<>();
        for (String line : output.split("\n")) {
            if (line.trim().isEmpty()) continue;

            String[] parts = line.split("\\|");
            if (parts.length >= 4) {
                String id = parts[0].trim();
                String name = parts[1].trim();
                String statusStr = parts[2].trim();
                String image = parts[3].trim();

                ContainerStatus.State state = parseDockerState(statusStr);
                containers.add(new ContainerStatus(
                        name, id, ContainerStatus.ContainerType.DOCKER, state, statusStr, image));
            }
        }
        return containers;
    }

    private ContainerStatus parseContainerInspect(String output) {
        String[] parts = output.trim().split("\\|");
        if (parts.length >= 4) {
            String id = parts[0].trim();
            String name = parts[1].trim().replaceFirst("^/", "");
            String status = parts[2].trim();
            String image = parts[3].trim();

            ContainerStatus.State state = switch (status.toLowerCase()) {
                case "running" -> ContainerStatus.State.RUNNING;
                case "exited" -> ContainerStatus.State.STOPPED;
                case "paused" -> ContainerStatus.State.PAUSED;
                case "created" -> ContainerStatus.State.STOPPED;
                default -> ContainerStatus.State.UNKNOWN;
            };

            return new ContainerStatus(name, id, ContainerStatus.ContainerType.DOCKER, state, status, image);
        }
        return ContainerStatus.unknown("unknown", ContainerStatus.ContainerType.DOCKER);
    }

    private ContainerStatus.State parseDockerState(String statusStr) {
        String lower = statusStr.toLowerCase();
        if (lower.startsWith("up")) return ContainerStatus.State.RUNNING;
        if (lower.startsWith("exited")) return ContainerStatus.State.STOPPED;
        if (lower.contains("paused")) return ContainerStatus.State.PAUSED;
        return ContainerStatus.State.UNKNOWN;
    }
}
