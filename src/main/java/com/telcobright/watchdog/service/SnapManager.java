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
public class SnapManager {

    private static final Logger LOG = Logger.getLogger(SnapManager.class);

    @Inject
    CommandExecutor executor;

    public CommandResult holdRefresh(String snapName) {
        LOG.infof("Holding refresh for snap: %s", snapName);
        return executor.executeSudo("snap refresh --hold=forever " + snapName);
    }

    public CommandResult unholdRefresh(String snapName) {
        LOG.infof("Unholding refresh for snap: %s", snapName);
        return executor.executeSudo("snap refresh --unhold " + snapName);
    }

    public boolean isRefreshHeld(String snapName) {
        CommandResult result = executor.execute("snap info " + snapName);
        if (!result.isSuccess()) {
            return false;
        }
        return result.stdout().contains("hold:");
    }

    public List<SnapInfo> listSnaps() {
        CommandResult result = executor.execute("snap list");

        if (!result.isSuccess()) {
            return List.of();
        }

        return parseSnapList(result.stdout());
    }

    public SnapInfo getSnapInfo(String snapName) {
        CommandResult result = executor.execute("snap info " + snapName);

        if (!result.isSuccess()) {
            return null;
        }

        return parseSnapInfo(snapName, result.stdout());
    }

    public CommandResult refreshSnap(String snapName) {
        LOG.infof("Refreshing snap: %s", snapName);
        return executor.executeSudo("snap refresh " + snapName);
    }

    public CommandResult restartSnap(String snapName) {
        LOG.infof("Restarting snap: %s", snapName);
        return executor.executeSudo("snap restart " + snapName);
    }

    public List<String> getPendingRefreshes() {
        CommandResult result = executor.execute("snap refresh --list");
        if (!result.isSuccess() || result.stdout().contains("All snaps up to date")) {
            return List.of();
        }

        List<String> pending = new ArrayList<>();
        for (String line : result.stdout().split("\n")) {
            if (line.trim().isEmpty() || line.startsWith("Name")) continue;
            String[] parts = line.trim().split("\\s+");
            if (parts.length > 0) {
                pending.add(parts[0]);
            }
        }
        return pending;
    }

    public String getLastRefreshTime(String snapName) {
        CommandResult result = executor.execute("snap changes | grep -i 'refresh.*" + snapName + "' | tail -1");
        if (result.isSuccess() && !result.stdout().trim().isEmpty()) {
            return result.stdout().trim();
        }
        return null;
    }

    private List<SnapInfo> parseSnapList(String output) {
        List<SnapInfo> snaps = new ArrayList<>();
        String[] lines = output.split("\n");

        for (int i = 1; i < lines.length; i++) { // Skip header
            String[] parts = lines[i].trim().split("\\s+");
            if (parts.length >= 4) {
                snaps.add(new SnapInfo(
                        parts[0], // name
                        parts[1], // version
                        parts[2], // revision
                        parts[3], // tracking
                        parts.length > 4 ? parts[4] : "" // publisher
                ));
            }
        }
        return snaps;
    }

    private SnapInfo parseSnapInfo(String name, String output) {
        String version = extractField(output, "installed:");
        String tracking = extractField(output, "tracking:");
        String refresh = extractField(output, "refresh-date:");

        if (version != null && version.contains(" ")) {
            version = version.split("\\s+")[0];
        }

        return new SnapInfo(name, version, null, tracking, null);
    }

    private String extractField(String output, String fieldName) {
        for (String line : output.split("\n")) {
            if (line.toLowerCase().startsWith(fieldName.toLowerCase())) {
                return line.substring(fieldName.length()).trim();
            }
        }
        return null;
    }

    public record SnapInfo(
            String name,
            String version,
            String revision,
            String tracking,
            String publisher
    ) {}
}
