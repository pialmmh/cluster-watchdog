package com.telcobright.watchdog.action;

import com.telcobright.watchdog.model.CommandResult;

/**
 * Base interface for all remediation actions.
 */
public interface Action {

    /**
     * Execute the action.
     * @return Result of the action execution
     */
    CommandResult execute();

    /**
     * Check if the action is needed.
     * @return true if the action should be executed
     */
    boolean isNeeded();

    /**
     * Get a description of what this action does.
     */
    String getDescription();
}
