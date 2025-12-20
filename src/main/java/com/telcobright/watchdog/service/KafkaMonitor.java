package com.telcobright.watchdog.service;

import com.telcobright.watchdog.model.ClusterHealth;
import com.telcobright.watchdog.model.CommandResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class KafkaMonitor {

    private static final Logger LOG = Logger.getLogger(KafkaMonitor.class);

    @Inject
    CommandExecutor executor;

    @Inject
    DockerMonitor dockerMonitor;

    public ClusterHealth.KafkaClusterStatus checkCluster(String bootstrapServer) {
        boolean controllerId = checkController(bootstrapServer);
        int availableBrokers = countAvailableBrokers(bootstrapServer);
        boolean topicsAccessible = checkTopicsAccessible(bootstrapServer);

        String message;
        if (availableBrokers == 0) {
            message = "Cluster unreachable";
        } else if (!controllerId) {
            message = "No controller elected";
        } else if (!topicsAccessible) {
            message = "Topics not accessible";
        } else {
            message = "Cluster healthy";
        }

        return new ClusterHealth.KafkaClusterStatus(
                3, // Assuming 3 brokers, this should come from config
                availableBrokers,
                controllerId,
                topicsAccessible,
                message
        );
    }

    public boolean isBrokerReachable(String brokerAddress) {
        String[] parts = brokerAddress.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        CommandResult result = executor.execute(
                "timeout 5 bash -c 'echo > /dev/tcp/" + host + "/" + port + "' 2>/dev/null");
        return result.isSuccess();
    }

    public boolean checkController(String bootstrapServer) {
        CommandResult result = executor.execute(
                "kafka-metadata.sh --snapshot /var/kafka-logs/__cluster_metadata-0/00000000000000000000.log " +
                        "--command controller 2>/dev/null || echo 'not found'");

        if (result.isSuccess() && !result.stdout().contains("not found")) {
            return true;
        }

        // Fallback: check if broker container is running with controller role
        return dockerMonitor.isContainerRunning("kafka-controller") ||
                dockerMonitor.isContainerRunning("kafka");
    }

    public int countAvailableBrokers(String bootstrapServer) {
        int count = 0;
        String[] servers = bootstrapServer.split(",");
        for (String server : servers) {
            if (isBrokerReachable(server.trim())) {
                count++;
            }
        }
        return count;
    }

    public boolean checkTopicsAccessible(String bootstrapServer) {
        // Simple connectivity check via TCP
        return isBrokerReachable(bootstrapServer.split(",")[0]);
    }

    public List<String> listTopics(String bootstrapServer) {
        CommandResult result = executor.execute(
                "kafka-topics.sh --bootstrap-server " + bootstrapServer + " --list 2>/dev/null");

        if (!result.isSuccess()) {
            return List.of();
        }

        List<String> topics = new ArrayList<>();
        for (String line : result.stdout().split("\n")) {
            if (!line.trim().isEmpty()) {
                topics.add(line.trim());
            }
        }
        return topics;
    }

    public TopicInfo describeTopic(String bootstrapServer, String topicName) {
        CommandResult result = executor.execute(
                "kafka-topics.sh --bootstrap-server " + bootstrapServer +
                        " --describe --topic " + topicName + " 2>/dev/null");

        if (!result.isSuccess()) {
            return null;
        }

        return parseTopicDescription(result.stdout());
    }

    public ConsumerGroupInfo describeConsumerGroup(String bootstrapServer, String groupId) {
        CommandResult result = executor.execute(
                "kafka-consumer-groups.sh --bootstrap-server " + bootstrapServer +
                        " --describe --group " + groupId + " 2>/dev/null");

        if (!result.isSuccess()) {
            return null;
        }

        return parseConsumerGroupDescription(result.stdout());
    }

    private TopicInfo parseTopicDescription(String output) {
        int partitions = 0;
        int replicationFactor = 0;

        Pattern partitionPattern = Pattern.compile("PartitionCount:\\s*(\\d+)");
        Pattern replicationPattern = Pattern.compile("ReplicationFactor:\\s*(\\d+)");

        Matcher partitionMatcher = partitionPattern.matcher(output);
        if (partitionMatcher.find()) {
            partitions = Integer.parseInt(partitionMatcher.group(1));
        }

        Matcher replicationMatcher = replicationPattern.matcher(output);
        if (replicationMatcher.find()) {
            replicationFactor = Integer.parseInt(replicationMatcher.group(1));
        }

        return new TopicInfo(partitions, replicationFactor);
    }

    private ConsumerGroupInfo parseConsumerGroupDescription(String output) {
        long totalLag = 0;
        int members = 0;

        for (String line : output.split("\n")) {
            if (line.contains("LAG") || line.trim().isEmpty()) continue;

            String[] parts = line.trim().split("\\s+");
            if (parts.length >= 6) {
                members++;
                try {
                    long lag = Long.parseLong(parts[5]);
                    totalLag += lag;
                } catch (NumberFormatException ignored) {}
            }
        }

        return new ConsumerGroupInfo(members, totalLag);
    }

    public record TopicInfo(int partitions, int replicationFactor) {}
    public record ConsumerGroupInfo(int members, long totalLag) {}
}
