package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;
import com.telcobright.watchdog.service.SnapManager;
import org.jboss.logging.Logger;

/**
 * Action to hold snap refresh for critical packages like LXD.
 * Prevents automatic updates that could disrupt cluster operations.
 */
public class HoldSnapRefreshAction implements Action {

    private static final Logger LOG = Logger.getLogger(HoldSnapRefreshAction.class);

    private final SnapManager snapManager;
    private final String snapName;

    public HoldSnapRefreshAction(SnapManager snapManager, String snapName) {
        this.snapManager = snapManager;
        this.snapName = snapName;
    }

    @Override
    public CommandResult execute() {
        if (!isNeeded()) {
            LOG.debugf("Snap %s refresh already held, skipping", snapName);
            return CommandResult.success("noop", "Refresh already held", java.time.Duration.ZERO);
        }

        LOG.infof("Holding snap refresh for: %s", snapName);
        return snapManager.holdRefresh(snapName);
    }

    @Override
    public boolean isNeeded() {
        return !snapManager.isRefreshHeld(snapName);
    }

    @Override
    public String getDescription() {
        return "Hold snap refresh for: " + snapName;
    }
}
