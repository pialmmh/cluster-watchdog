package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.CommandResult;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class CommandExecutor {

    private static final Logger LOG = Logger.getLogger(CommandExecutor.class);
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    public CommandResult execute(String command) {
        return execute(command, DEFAULT_TIMEOUT_SECONDS);
    }

    public CommandResult execute(String command, int timeoutSeconds) {
        LOG.debugf("Executing command: %s", command);
        Instant start = Instant.now();

        try {
            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
            pb.redirectErrorStream(false);

            Process process = pb.start();

            String stdout;
            String stderr;

            try (BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                 BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {

                stdout = stdoutReader.lines().collect(Collectors.joining("\n"));
                stderr = stderrReader.lines().collect(Collectors.joining("\n"));
            }

            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            Duration duration = Duration.between(start, Instant.now());

            if (!finished) {
                process.destroyForcibly();
                return CommandResult.failure(command, -1, stdout, "Command timed out after " + timeoutSeconds + "s", duration);
            }

            int exitCode = process.exitValue();
            if (exitCode == 0) {
                LOG.debugf("Command succeeded: %s", command);
                return CommandResult.success(command, stdout, duration);
            } else {
                LOG.warnf("Command failed with exit code %d: %s", exitCode, command);
                return CommandResult.failure(command, exitCode, stdout, stderr, duration);
            }

        } catch (Exception e) {
            LOG.errorf(e, "Error executing command: %s", command);
            return CommandResult.error(command, e.getMessage());
        }
    }

    public CommandResult executeSudo(String command) {
        return execute("sudo " + command);
    }

    public boolean commandExists(String command) {
        return execute("which " + command).isSuccess();
    }
}
