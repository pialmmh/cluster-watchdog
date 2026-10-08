package com.telcobright.watchdog.service;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.model.BgpRoutingStatus;
import com.telcobright.watchdog.model.BgpRoutingStatus.RouteCheck;
import com.telcobright.watchdog.model.BgpRoutingStatus.RouteType;
import com.telcobright.watchdog.model.BgpRoutingStatus.RoutingHealth;
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

    @Inject
    ClusterConfig config;

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

    /**
     * Get all routes from kernel routing table (includes BGP, static, connected).
     */
    public List<KernelRoute> getAllRoutes(String containerName) {
        CommandResult result;
        if (containerName != null && !containerName.isEmpty()) {
            result = lxcMonitor.execInContainer(containerName, "ip route show");
        } else {
            result = executor.execute("ip route show");
        }

        if (!result.isSuccess()) {
            return List.of();
        }

        return parseKernelRoutes(result.stdout());
    }

    /**
     * Get comprehensive routing status with expected route validation.
     */
    public BgpRoutingStatus getRoutingStatus(String containerName) {
        boolean frrRunning = isFrrRunning(containerName);
        if (!frrRunning) {
            return BgpRoutingStatus.frrNotRunning();
        }

        List<BgpNeighbor> neighbors = getNeighbors(containerName);
        int totalPeers = neighbors.size();
        int establishedPeers = (int) neighbors.stream()
                .filter(n -> "Established".equalsIgnoreCase(n.state) || isNumericState(n.state))
                .count();

        List<BgpRoute> bgpRoutes = getRoutes(containerName);
        List<KernelRoute> allRoutes = getAllRoutes(containerName);

        // Collect expected routes
        List<ExpectedRoute> expectedRoutes = collectExpectedRoutes();
        List<RouteCheck> routeChecks = validateRoutes(expectedRoutes, allRoutes, bgpRoutes);

        int missingCount = (int) routeChecks.stream().filter(r -> !r.present()).count();
        int totalExpected = routeChecks.size();

        String message;
        RoutingHealth health;

        if (missingCount == 0) {
            health = RoutingHealth.HEALTHY;
            message = String.format("All %d expected routes present, %d BGP routes, %d/%d peers",
                    totalExpected, bgpRoutes.size(), establishedPeers, totalPeers);
        } else if (missingCount <= totalExpected / 2) {
            health = RoutingHealth.DEGRADED;
            message = String.format("%d/%d routes missing, %d/%d peers established",
                    missingCount, totalExpected, establishedPeers, totalPeers);
        } else {
            health = RoutingHealth.UNHEALTHY;
            message = String.format("%d/%d routes missing - routing severely degraded",
                    missingCount, totalExpected);
        }

        return new BgpRoutingStatus(
                true, totalPeers, establishedPeers, bgpRoutes.size(),
                routeChecks, health, message, java.time.Instant.now()
        );
    }

    /**
     * Collect all expected routes from cluster configuration.
     */
    private List<ExpectedRoute> collectExpectedRoutes() {
        List<ExpectedRoute> expected = new ArrayList<>();
        int localNodeId = config.local().nodeId();

        // Add container subnets from all OTHER nodes in the cluster
        config.nodes().forEach((key, nodeConfig) -> {
            if (nodeConfig.id() != localNodeId) {
                nodeConfig.containerSubnet().ifPresent(subnet ->
                        expected.add(new ExpectedRoute(subnet, RouteType.CONTAINER_SUBNET))
                );
            }
        });

        // Add explicitly configured expected container subnets
        config.local().expectedContainerSubnets().ifPresent(subnets ->
                subnets.forEach(subnet -> expected.add(new ExpectedRoute(subnet, RouteType.CONTAINER_SUBNET)))
        );

        // Add explicitly configured wireguard subnets
        config.local().expectedWireguardSubnets().ifPresent(subnets ->
                subnets.forEach(subnet -> expected.add(new ExpectedRoute(subnet, RouteType.WIREGUARD_OVERLAY)))
        );

        // Add global BGP config routes
        config.bgp().ifPresent(bgpConfig -> {
            bgpConfig.wireguardSubnet().ifPresent(subnet ->
                    expected.add(new ExpectedRoute(subnet, RouteType.WIREGUARD_OVERLAY))
            );
            bgpConfig.vpnClientSubnet().ifPresent(subnet ->
                    expected.add(new ExpectedRoute(subnet, RouteType.VPN_CLIENT))
            );
            bgpConfig.additionalExpectedRoutes().ifPresent(routes ->
                    routes.forEach(route -> expected.add(new ExpectedRoute(route, RouteType.CUSTOM)))
            );
        });

        return expected;
    }

    /**
     * Validate expected routes against actual routes.
     */
    private List<RouteCheck> validateRoutes(List<ExpectedRoute> expected,
                                            List<KernelRoute> kernelRoutes,
                                            List<BgpRoute> bgpRoutes) {
        List<RouteCheck> checks = new ArrayList<>();

        for (ExpectedRoute exp : expected) {
            // Check kernel routing table first
            KernelRoute kernelMatch = findMatchingKernelRoute(exp.subnet, kernelRoutes);
            if (kernelMatch != null) {
                checks.add(new RouteCheck(
                        exp.subnet, exp.type, true,
                        kernelMatch.gateway() != null ? kernelMatch.gateway() : kernelMatch.device(),
                        kernelMatch.protocol()
                ));
                continue;
            }

            // Check BGP routes (may not be in kernel yet or different format)
            BgpRoute bgpMatch = findMatchingBgpRoute(exp.subnet, bgpRoutes);
            if (bgpMatch != null) {
                checks.add(new RouteCheck(
                        exp.subnet, exp.type, true,
                        bgpMatch.nextHop(), "bgp"
                ));
                continue;
            }

            // Route not found
            checks.add(new RouteCheck(exp.subnet, exp.type, false, null, null));
        }

        return checks;
    }

    private KernelRoute findMatchingKernelRoute(String targetSubnet, List<KernelRoute> routes) {
        String normalizedTarget = normalizeSubnet(targetSubnet);
        for (KernelRoute route : routes) {
            if (normalizeSubnet(route.destination()).equals(normalizedTarget)) {
                return route;
            }
        }
        return null;
    }

    private BgpRoute findMatchingBgpRoute(String targetSubnet, List<BgpRoute> routes) {
        String normalizedTarget = normalizeSubnet(targetSubnet);
        for (BgpRoute route : routes) {
            if (normalizeSubnet(route.prefix()).equals(normalizedTarget)) {
                return route;
            }
        }
        return null;
    }

    private String normalizeSubnet(String subnet) {
        // Handle "10.10.199.0/24" vs "10.10.199.0 /24" etc.
        return subnet.replaceAll("\\s+", "").trim();
    }

    private boolean isNumericState(String state) {
        // BGP shows prefix count when established
        try {
            Integer.parseInt(state);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private List<KernelRoute> parseKernelRoutes(String output) {
        List<KernelRoute> routes = new ArrayList<>();
        // Format: 10.10.197.0/24 via 10.9.9.1 dev wg0 proto bgp metric 20
        // Or: 10.10.199.0/24 dev lxdbr0 proto kernel scope link src 10.10.199.1
        Pattern routePattern = Pattern.compile(
                "^(\\S+)(?:\\s+via\\s+(\\S+))?\\s+dev\\s+(\\S+)(?:\\s+proto\\s+(\\S+))?");

        for (String line : output.split("\n")) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("default")) {
                continue;
            }

            Matcher matcher = routePattern.matcher(line);
            if (matcher.find()) {
                routes.add(new KernelRoute(
                        matcher.group(1), // destination
                        matcher.group(2), // gateway (may be null)
                        matcher.group(3), // device
                        matcher.group(4) != null ? matcher.group(4) : "unknown" // protocol
                ));
            }
        }
        return routes;
    }

    private record ExpectedRoute(String subnet, RouteType type) {}

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

    public record KernelRoute(
            String destination,
            String gateway,
            String device,
            String protocol
    ) {}
}
