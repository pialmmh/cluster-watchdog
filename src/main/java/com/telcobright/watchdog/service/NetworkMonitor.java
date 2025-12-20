package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.model.NetworkStatus;
import com.telcobright.watchdog.model.NodeStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class NetworkMonitor {

    private static final Logger LOG = Logger.getLogger(NetworkMonitor.class);
    private static final Pattern IP_PATTERN = Pattern.compile("inet\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+/\\d+)");

    @Inject
    CommandExecutor executor;

    public NetworkStatus checkBridge(String bridgeName) {
        CommandResult result = executor.execute("ip addr show " + bridgeName);

        if (!result.isSuccess()) {
            return NetworkStatus.down(bridgeName, NetworkStatus.InterfaceType.BRIDGE,
                    "Interface not found: " + result.stderr());
        }

        List<String> ips = extractIpAddresses(result.stdout());
        boolean isUp = result.stdout().contains("state UP") || result.stdout().contains("LOWER_UP");

        if (ips.isEmpty()) {
            return NetworkStatus.degraded(bridgeName, NetworkStatus.InterfaceType.BRIDGE, ips,
                    "No IP addresses assigned");
        }

        if (!isUp) {
            return NetworkStatus.degraded(bridgeName, NetworkStatus.InterfaceType.BRIDGE, ips,
                    "Interface is DOWN");
        }

        return NetworkStatus.up(bridgeName, NetworkStatus.InterfaceType.BRIDGE, ips);
    }

    public boolean hasIpAddress(String interfaceName, String expectedIp) {
        CommandResult result = executor.execute("ip addr show " + interfaceName);
        if (!result.isSuccess()) {
            return false;
        }
        return result.stdout().contains(expectedIp);
    }

    public CommandResult addIpAddress(String interfaceName, String ip) {
        LOG.infof("Adding IP %s to interface %s", ip, interfaceName);
        return executor.executeSudo("ip addr add " + ip + " dev " + interfaceName);
    }

    public NodeStatus.PeerConnectivity pingPeer(String peerIp) {
        CommandResult result = executor.execute("ping -c 3 -W 2 " + peerIp);

        if (!result.isSuccess()) {
            return new NodeStatus.PeerConnectivity(peerIp, false, 100.0, 0.0);
        }

        double packetLoss = extractPacketLoss(result.stdout());
        double latency = extractLatency(result.stdout());

        return new NodeStatus.PeerConnectivity(peerIp, packetLoss < 100, packetLoss, latency);
    }

    public boolean isPortListening(String ip, int port) {
        CommandResult result = executor.executeSudo("ss -tlnp | grep " + ip + ":" + port);
        return result.isSuccess() && result.stdout().contains("LISTEN");
    }

    private List<String> extractIpAddresses(String output) {
        List<String> ips = new ArrayList<>();
        Matcher matcher = IP_PATTERN.matcher(output);
        while (matcher.find()) {
            ips.add(matcher.group(1));
        }
        return ips;
    }

    private double extractPacketLoss(String output) {
        Pattern pattern = Pattern.compile("(\\d+)% packet loss");
        Matcher matcher = pattern.matcher(output);
        if (matcher.find()) {
            return Double.parseDouble(matcher.group(1));
        }
        return 100.0;
    }

    private double extractLatency(String output) {
        Pattern pattern = Pattern.compile("rtt min/avg/max/mdev = [\\d.]+/([\\d.]+)/");
        Matcher matcher = pattern.matcher(output);
        if (matcher.find()) {
            return Double.parseDouble(matcher.group(1));
        }
        return 0.0;
    }
}
