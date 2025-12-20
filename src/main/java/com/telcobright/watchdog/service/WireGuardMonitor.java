package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.model.NetworkStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class WireGuardMonitor {

    private static final Logger LOG = Logger.getLogger(WireGuardMonitor.class);
    private static final Pattern PEER_PATTERN = Pattern.compile("peer:\\s*(\\S+)");
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile("endpoint:\\s*(\\S+)");
    private static final Pattern HANDSHAKE_PATTERN = Pattern.compile("latest handshake:\\s*(.+)");
    private static final Pattern TRANSFER_PATTERN = Pattern.compile("transfer:\\s*([\\d.]+\\s*\\w+)\\s+received,\\s*([\\d.]+\\s*\\w+)\\s+sent");

    @Inject
    CommandExecutor executor;

    public NetworkStatus checkInterface(String interfaceName) {
        CommandResult result = executor.executeSudo("wg show " + interfaceName);

        if (!result.isSuccess()) {
            return NetworkStatus.down(interfaceName, NetworkStatus.InterfaceType.WIREGUARD,
                    "Interface not active: " + result.stderr());
        }

        List<PeerInfo> peers = parsePeers(result.stdout());
        int activePeers = (int) peers.stream().filter(PeerInfo::isActive).count();

        if (peers.isEmpty()) {
            return NetworkStatus.degraded(interfaceName, NetworkStatus.InterfaceType.WIREGUARD,
                    List.of(), "No peers configured");
        }

        if (activePeers == 0) {
            return NetworkStatus.degraded(interfaceName, NetworkStatus.InterfaceType.WIREGUARD,
                    List.of(), "No active peers (0/" + peers.size() + ")");
        }

        if (activePeers < peers.size()) {
            return NetworkStatus.degraded(interfaceName, NetworkStatus.InterfaceType.WIREGUARD,
                    List.of(), activePeers + "/" + peers.size() + " peers active");
        }

        return NetworkStatus.up(interfaceName, NetworkStatus.InterfaceType.WIREGUARD, List.of());
    }

    public boolean isInterfaceUp(String interfaceName) {
        CommandResult result = executor.executeSudo("wg show " + interfaceName);
        return result.isSuccess();
    }

    public CommandResult startInterface(String interfaceName) {
        LOG.infof("Starting WireGuard interface: %s", interfaceName);
        return executor.executeSudo("wg-quick up " + interfaceName);
    }

    public CommandResult stopInterface(String interfaceName) {
        LOG.infof("Stopping WireGuard interface: %s", interfaceName);
        return executor.executeSudo("wg-quick down " + interfaceName);
    }

    public CommandResult restartInterface(String interfaceName) {
        LOG.infof("Restarting WireGuard interface: %s", interfaceName);
        CommandResult down = executor.executeSudo("wg-quick down " + interfaceName);
        return executor.executeSudo("wg-quick up " + interfaceName);
    }

    public List<PeerInfo> getPeers(String interfaceName) {
        CommandResult result = executor.executeSudo("wg show " + interfaceName);
        if (!result.isSuccess()) {
            return List.of();
        }
        return parsePeers(result.stdout());
    }

    private List<PeerInfo> parsePeers(String output) {
        List<PeerInfo> peers = new ArrayList<>();
        String[] sections = output.split("peer:");

        for (int i = 1; i < sections.length; i++) {
            String section = "peer:" + sections[i];
            PeerInfo peer = parsePeerSection(section);
            if (peer != null) {
                peers.add(peer);
            }
        }
        return peers;
    }

    private PeerInfo parsePeerSection(String section) {
        Matcher peerMatcher = PEER_PATTERN.matcher(section);
        if (!peerMatcher.find()) return null;

        String publicKey = peerMatcher.group(1);
        String endpoint = null;
        String handshake = null;
        String received = null;
        String sent = null;

        Matcher endpointMatcher = ENDPOINT_PATTERN.matcher(section);
        if (endpointMatcher.find()) {
            endpoint = endpointMatcher.group(1);
        }

        Matcher handshakeMatcher = HANDSHAKE_PATTERN.matcher(section);
        if (handshakeMatcher.find()) {
            handshake = handshakeMatcher.group(1).trim();
        }

        Matcher transferMatcher = TRANSFER_PATTERN.matcher(section);
        if (transferMatcher.find()) {
            received = transferMatcher.group(1);
            sent = transferMatcher.group(2);
        }

        boolean active = handshake != null && !handshake.isEmpty();
        return new PeerInfo(publicKey, endpoint, handshake, received, sent, active);
    }

    public record PeerInfo(
            String publicKey,
            String endpoint,
            String lastHandshake,
            String received,
            String sent,
            boolean isActive
    ) {}
}
