package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class BgpMonitor {

    private static final Logger LOG = Logger.getLogger(BgpMonitor.class);

    @Inject
    CommandExecutor executor;

    @Inject
    LxcMonitor lxcMonitor;

    public boolean isFrrRunning(String containerName) {
        if (containerName != null && !containerName.isEmpty()) {
            CommandResult result = lxcMonitor.execInContainer(containerName,
                    "systemctl is-active frr");
            return result.isSuccess() && result.stdout().trim().equals("active");
        }

        CommandResult result = executor.execute("systemctl is-active frr");
        return result.isSuccess() && result.stdout().trim().equals("active");
    }

    public List<BgpNeighbor> getNeighbors(String containerName) {
        CommandResult result;
        if (containerName != null && !containerName.isEmpty()) {
            result = lxcMonitor.execInContainer(containerName, "vtysh -c 'show bgp summary'");
        } else {
            result = executor.executeSudo("vtysh -c 'show bgp summary'");
        }

        if (!result.isSuccess()) {
            return List.of();
        }

        return parseNeighborSummary(result.stdout());
    }

    public List<BgpRoute> getRoutes(String containerName) {
        CommandResult result;
        if (containerName != null && !containerName.isEmpty()) {
            result = lxcMonitor.execInContainer(containerName, "vtysh -c 'show ip route bgp'");
        } else {
            result = executor.executeSudo("vtysh -c 'show ip route bgp'");
        }

        if (!result.isSuccess()) {
            return List.of();
        }

        return parseBgpRoutes(result.stdout());
    }

    public BgpStatus getStatus(String containerName) {
        boolean frrRunning = isFrrRunning(containerName);
        if (!frrRunning) {
            return new BgpStatus(false, 0, 0, 0, "FRR not running");
        }

        List<BgpNeighbor> neighbors = getNeighbors(containerName);
        int totalPeers = neighbors.size();
        int establishedPeers = (int) neighbors.stream()
                .filter(n -> "Established".equalsIgnoreCase(n.state))
                .count();

        List<BgpRoute> routes = getRoutes(containerName);

        String status;
        if (establishedPeers == 0 && totalPeers > 0) {
            status = "No peers established";
        } else if (establishedPeers < totalPeers) {
            status = establishedPeers + "/" + totalPeers + " peers established";
        } else if (routes.isEmpty()) {
            status = "No routes learned";
        } else {
            status = "Healthy - " + routes.size() + " routes";
        }

        return new BgpStatus(true, totalPeers, establishedPeers, routes.size(), status);
    }

    public CommandResult restartFrr(String containerName) {
        LOG.infof("Restarting FRR in container: %s", containerName);
        if (containerName != null && !containerName.isEmpty()) {
            return lxcMonitor.execInContainer(containerName, "systemctl restart frr");
        }
        return executor.executeSudo("systemctl restart frr");
    }

    private List<BgpNeighbor> parseNeighborSummary(String output) {
        List<BgpNeighbor> neighbors = new ArrayList<>();
        Pattern neighborPattern = Pattern.compile(
                "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+\\d+\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\S+)\\s+(\\S+)");

        for (String line : output.split("\n")) {
            Matcher matcher = neighborPattern.matcher(line.trim());
            if (matcher.find()) {
                neighbors.add(new BgpNeighbor(
                        matcher.group(1), // IP
                        Integer.parseInt(matcher.group(2)), // AS
                        matcher.group(8), // State or PfxRcd
                        Integer.parseInt(matcher.group(5)), // MsgRcvd
                        Integer.parseInt(matcher.group(6))  // MsgSent
                ));
            }
        }
        return neighbors;
    }

    private List<BgpRoute> parseBgpRoutes(String output) {
        List<BgpRoute> routes = new ArrayList<>();
        Pattern routePattern = Pattern.compile(
                "B>?\\*?\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+/\\d+).*via\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)");

        for (String line : output.split("\n")) {
            Matcher matcher = routePattern.matcher(line);
            if (matcher.find()) {
                routes.add(new BgpRoute(
                        matcher.group(1), // Prefix
                        matcher.group(2), // Next hop
                        line.contains("*") // Best route
                ));
            }
        }
        return routes;
    }

    public record BgpNeighbor(
            String ip,
            int asNumber,
            String state,
            int messagesReceived,
            int messagesSent
    ) {}

    public record BgpRoute(
            String prefix,
            String nextHop,
            boolean isBest
    ) {}

    public record BgpStatus(
            boolean frrRunning,
            int totalPeers,
            int establishedPeers,
            int routeCount,
            String message
    ) {}
}
