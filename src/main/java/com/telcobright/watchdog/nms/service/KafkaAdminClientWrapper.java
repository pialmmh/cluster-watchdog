package com.telcobright.watchdog.nms.service;

import com.telcobright.watchdog.nms.dto.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Wrapper around Kafka AdminClient providing monitoring operations.
 */
public class KafkaAdminClientWrapper implements AutoCloseable {

    private final AdminClient adminClient;
    private final String clusterId;
    private final String bootstrapServers;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    public KafkaAdminClientWrapper(String clusterId, String bootstrapServers, String securityProtocol) {
        this.clusterId = clusterId;
        this.bootstrapServers = bootstrapServers;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 5000);

        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }

        this.adminClient = AdminClient.create(props);
    }

    /**
     * Get cluster overview information.
     */
    public ClusterOverview getClusterOverview(String tenantName, String profile) {
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();

            String kafkaClusterId = clusterResult.clusterId().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            Collection<Node> nodes = clusterResult.nodes().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            Node controller = clusterResult.controller().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            int onlineBrokers = nodes.size();
            int totalBrokers = countBootstrapServers(bootstrapServers);
            int downBrokers = totalBrokers - onlineBrokers;

            List<BootstrapServerStatus> brokerStatuses = buildBrokerStatuses(bootstrapServers, nodes);

            String status;
            if (onlineBrokers == 0) {
                status = "Down";
            } else if (totalBrokers > 0 && (onlineBrokers * 100.0 / totalBrokers) >= 50) {
                status = "Healthy";
            } else {
                status = "Down";
            }

            return ClusterOverview.builder()
                    .clusterId(this.clusterId)
                    .clusterStatus(status)
                    .totalBrokers(totalBrokers)
                    .onlineBrokers(onlineBrokers)
                    .upBrokers(onlineBrokers)
                    .downBrokers(downBrokers)
                    .controllerId(controller != null ? controller.id() : -1)
                    .tenantName(tenantName)
                    .profile(profile)
                    .bootstrapServers(bootstrapServers)
                    .brokerStatuses(brokerStatuses)
                    .build();

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            int totalBrokers = countBootstrapServers(bootstrapServers);
            List<BootstrapServerStatus> brokerStatuses = buildBrokerStatuses(bootstrapServers, Collections.emptyList());
            return ClusterOverview.builder()
                    .clusterId(clusterId)
                    .clusterStatus("Down")
                    .totalBrokers(totalBrokers)
                    .onlineBrokers(0)
                    .upBrokers(0)
                    .downBrokers(totalBrokers)
                    .controllerId(-1)
                    .tenantName(tenantName)
                    .profile(profile)
                    .bootstrapServers(bootstrapServers)
                    .brokerStatuses(brokerStatuses)
                    .build();
        }
    }

    /**
     * Get broker status for all brokers in the cluster.
     */
    public List<BrokerStatus> getBrokerStatus() {
        List<BrokerStatus> brokers = new ArrayList<>();

        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            Node controller = clusterResult.controller().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            int controllerId = controller != null ? controller.id() : -1;

            for (Node node : nodes) {
                brokers.add(BrokerStatus.builder()
                        .brokerId(node.id())
                        .host(node.host())
                        .port(node.port())
                        .status("Online")
                        .rack(node.rack() != null ? node.rack() : "N/A")
                        .isController(node.id() == controllerId)
                        .build());
            }

            brokers.sort(Comparator.comparingInt(BrokerStatus::getBrokerId));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting broker status: " + e.getMessage());
        }

        return brokers;
    }

    /**
     * Get topic health information for all topics.
     */
    public List<TopicHealth> getTopicHealth() {
        List<TopicHealth> topics = new ArrayList<>();

        try {
            Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().listInternal(true))
                    .names()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            if (topicNames.isEmpty()) {
                return topics;
            }

            Map<String, TopicDescription> descriptions = adminClient.describeTopics(topicNames)
                    .allTopicNames()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                TopicDescription desc = entry.getValue();
                List<TopicPartitionInfo> partitions = desc.partitions();

                int underReplicated = 0;
                int offline = 0;
                int replicationFactor = 0;

                for (TopicPartitionInfo partition : partitions) {
                    if (replicationFactor == 0) {
                        replicationFactor = partition.replicas().size();
                    }

                    if (partition.leader() == null || partition.leader().id() == -1) {
                        offline++;
                    }

                    if (partition.isr().size() < partition.replicas().size()) {
                        underReplicated++;
                    }
                }

                topics.add(TopicHealth.builder()
                        .topicName(desc.name())
                        .partitions(partitions.size())
                        .underReplicatedPartitions(underReplicated)
                        .offlinePartitions(offline)
                        .replicationFactor(replicationFactor)
                        .internal(desc.isInternal())
                        .build());
            }

            topics.sort(Comparator.comparing(TopicHealth::getTopicName));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting topic health: " + e.getMessage());
        }

        return topics;
    }

    /**
     * Get consumer group information.
     */
    public List<ConsumerGroupInfo> getConsumerGroups() {
        List<ConsumerGroupInfo> groups = new ArrayList<>();

        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups()
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            if (listings.isEmpty()) {
                return groups;
            }

            List<String> groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());

            Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds)
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
                ConsumerGroupDescription desc = entry.getValue();

                Set<String> topics = desc.members().stream()
                        .flatMap(m -> m.assignment().topicPartitions().stream())
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet());

                Node coordinator = desc.coordinator();

                groups.add(ConsumerGroupInfo.builder()
                        .groupId(desc.groupId())
                        .state(desc.state().toString())
                        .memberCount(desc.members().size())
                        .topics(new ArrayList<>(topics))
                        .coordinatorId(coordinator != null ? coordinator.id() : -1)
                        .coordinatorHost(coordinator != null ? coordinator.host() : "N/A")
                        .build());
            }

            groups.sort(Comparator.comparing(ConsumerGroupInfo::getGroupId));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting consumer groups: " + e.getMessage());
        }

        return groups;
    }

    /**
     * Get consumer lag for all consumer groups.
     */
    public List<ConsumerLag> getConsumerLags() {
        List<ConsumerLag> lags = new ArrayList<>();

        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups()
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (ConsumerGroupListing listing : listings) {
                String groupId = listing.groupId();

                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

                    if (offsets.isEmpty()) continue;

                    Set<TopicPartition> partitions = offsets.keySet();
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
                            .listOffsets(partitions.stream()
                                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                            .all()
                            .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        long currentOffset = entry.getValue().offset();
                        long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : currentOffset;
                        long lag = Math.max(0, endOffset - currentOffset);

                        lags.add(ConsumerLag.builder()
                                .groupId(groupId)
                                .topic(tp.topic())
                                .partition(tp.partition())
                                .currentOffset(currentOffset)
                                .endOffset(endOffset)
                                .lag(lag)
                                .build());
                    }

                } catch (Exception e) {
                    System.err.println("[NMS] Error getting lag for group " + groupId + ": " + e.getMessage());
                }
            }

            lags.sort((a, b) -> Long.compare(b.getLag(), a.getLag()));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting consumer lags: " + e.getMessage());
        }

        return lags;
    }

    /**
     * Get consumer lag organized by topic with consumer group status.
     */
    public List<TopicConsumerLagSummary> getConsumerLagByTopic() {
        Map<String, List<TopicConsumerDetails>> topicConsumerMap = new LinkedHashMap<>();

        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups()
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            if (listings.isEmpty()) {
                return new ArrayList<>();
            }

            List<String> groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());

            Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds)
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (String groupId : groupIds) {
                try {
                    ConsumerGroupDescription desc = descriptions.get(groupId);
                    String status = desc != null ? desc.state().toString() : "Unknown";
                    int memberCount = desc != null ? desc.members().size() : 0;

                    Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

                    if (offsets.isEmpty()) continue;

                    Set<TopicPartition> partitions = offsets.keySet();
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
                            .listOffsets(partitions.stream()
                                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                            .all()
                            .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

                    Map<String, List<ConsumerPartitionLag>> partitionsByTopic = new HashMap<>();

                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        long currentOffset = entry.getValue().offset();
                        long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : currentOffset;
                        long lag = Math.max(0, endOffset - currentOffset);

                        ConsumerPartitionLag partitionLag = ConsumerPartitionLag.builder()
                                .partition(tp.partition())
                                .currentOffset(currentOffset)
                                .endOffset(endOffset)
                                .lag(lag)
                                .build();

                        partitionsByTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(partitionLag);
                    }

                    for (Map.Entry<String, List<ConsumerPartitionLag>> topicEntry : partitionsByTopic.entrySet()) {
                        String topicName = topicEntry.getKey();
                        List<ConsumerPartitionLag> partitionLags = topicEntry.getValue();

                        long totalLag = partitionLags.stream()
                                .mapToLong(ConsumerPartitionLag::getLag)
                                .sum();

                        partitionLags.sort(Comparator.comparingInt(ConsumerPartitionLag::getPartition));

                        TopicConsumerDetails consumerDetails = TopicConsumerDetails.builder()
                                .consumerGroupId(groupId)
                                .status(status)
                                .memberCount(memberCount)
                                .totalLag(totalLag)
                                .partitions(partitionLags)
                                .build();

                        topicConsumerMap.computeIfAbsent(topicName, k -> new ArrayList<>()).add(consumerDetails);
                    }

                } catch (Exception e) {
                    System.err.println("[NMS] Error getting lag for group " + groupId + ": " + e.getMessage());
                }
            }

            List<TopicConsumerLagSummary> result = new ArrayList<>();

            for (Map.Entry<String, List<TopicConsumerDetails>> entry : topicConsumerMap.entrySet()) {
                String topicName = entry.getKey();
                List<TopicConsumerDetails> consumers = entry.getValue();

                long totalTopicLag = consumers.stream()
                        .mapToLong(TopicConsumerDetails::getTotalLag)
                        .sum();

                consumers.sort((a, b) -> Long.compare(b.getTotalLag(), a.getTotalLag()));

                TopicConsumerLagSummary summary = TopicConsumerLagSummary.builder()
                        .topicName(topicName)
                        .consumerCount(consumers.size())
                        .totalLag(totalTopicLag)
                        .consumers(consumers)
                        .build();

                result.add(summary);
            }

            result.sort((a, b) -> Long.compare(b.getTotalLag(), a.getTotalLag()));

            return result;

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting consumer lag by topic: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Get active client connections.
     */
    public List<ActiveConnection> getActiveConnections() {
        List<ActiveConnection> connections = new ArrayList<>();

        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups()
                    .all()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            List<String> groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());

            if (!groupIds.isEmpty()) {
                Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds)
                        .all()
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

                for (ConsumerGroupDescription desc : descriptions.values()) {
                    for (MemberDescription member : desc.members()) {
                        String clientId = member.clientId();
                        String host = member.host();

                        if (host != null && host.startsWith("/")) {
                            host = host.substring(1);
                        }

                        connections.add(ActiveConnection.builder()
                                .clientId(clientId)
                                .clientType("Consumer")
                                .clientHost(host != null ? host : "unknown")
                                .connectedSince(Instant.now())
                                .connectedSinceFormatted("N/A")
                                .build());
                    }
                }
            }

            connections.sort(Comparator.comparing(ActiveConnection::getClientId));

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting active connections: " + e.getMessage());
        }

        return connections;
    }

    /**
     * Get storage information for all brokers.
     */
    public ClusterStorage getClusterStorage() {
        List<BrokerStorage> brokerStorages = new ArrayList<>();
        long totalClusterSize = 0;

        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            if (nodes.isEmpty()) {
                return ClusterStorage.builder()
                        .clusterId(clusterId)
                        .totalUsedBytes(0)
                        .totalUsedFormatted("0 B")
                        .brokerCount(0)
                        .brokers(Collections.emptyList())
                        .build();
            }

            List<Integer> brokerIds = nodes.stream()
                    .map(Node::id)
                    .collect(Collectors.toList());

            Map<Integer, Node> brokerNodeMap = nodes.stream()
                    .collect(Collectors.toMap(Node::id, n -> n));

            DescribeLogDirsResult logDirsResult = adminClient.describeLogDirs(brokerIds);
            Map<Integer, Map<String, LogDirDescription>> allLogDirs = logDirsResult.allDescriptions()
                    .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (Map.Entry<Integer, Map<String, LogDirDescription>> brokerEntry : allLogDirs.entrySet()) {
                int brokerId = brokerEntry.getKey();
                Map<String, LogDirDescription> logDirs = brokerEntry.getValue();

                List<LogDirInfo> logDirInfos = new ArrayList<>();
                List<String> storagePaths = new ArrayList<>();
                long brokerTotalSize = 0;
                String brokerStatus = "Online";

                for (Map.Entry<String, LogDirDescription> logDirEntry : logDirs.entrySet()) {
                    String logDirPath = logDirEntry.getKey();
                    LogDirDescription logDirDesc = logDirEntry.getValue();

                    storagePaths.add(logDirPath);

                    if (logDirDesc.error() != null) {
                        logDirInfos.add(LogDirInfo.builder()
                                .path(logDirPath)
                                .usedBytes(0)
                                .usedFormatted("N/A")
                                .error(logDirDesc.error().getMessage())
                                .partitions(Collections.emptyList())
                                .build());
                        continue;
                    }

                    List<TopicPartitionStorage> partitionStorages = new ArrayList<>();
                    long logDirSize = 0;

                    Map<TopicPartition, ReplicaInfo> replicaInfos = logDirDesc.replicaInfos();
                    for (Map.Entry<TopicPartition, ReplicaInfo> replicaEntry : replicaInfos.entrySet()) {
                        TopicPartition tp = replicaEntry.getKey();
                        ReplicaInfo replicaInfo = replicaEntry.getValue();

                        long partitionSize = replicaInfo.size();
                        logDirSize += partitionSize;

                        partitionStorages.add(TopicPartitionStorage.builder()
                                .topic(tp.topic())
                                .partition(tp.partition())
                                .sizeBytes(partitionSize)
                                .sizeFormatted(formatBytes(partitionSize))
                                .offsetLag(replicaInfo.offsetLag())
                                .build());
                    }

                    partitionStorages.sort((a, b) -> Long.compare(b.getSizeBytes(), a.getSizeBytes()));

                    logDirInfos.add(LogDirInfo.builder()
                            .path(logDirPath)
                            .usedBytes(logDirSize)
                            .usedFormatted(formatBytes(logDirSize))
                            .partitions(partitionStorages)
                            .build());

                    brokerTotalSize += logDirSize;
                }

                Node brokerNode = brokerNodeMap.get(brokerId);
                String brokerHost = brokerNode != null ? brokerNode.host() : "unknown";
                int brokerPort = brokerNode != null ? brokerNode.port() : -1;

                // Fetch filesystem metrics from node_exporter
                Map<String, String> nodeMetrics = fetchNodeExporterMetrics(brokerHost);

                // Build broker storage with filesystem information
                BrokerStorage.BrokerStorageBuilder builder = BrokerStorage.builder()
                        .brokerId(brokerId)
                        .host(brokerHost)
                        .port(brokerPort)
                        .storagePaths(storagePaths)
                        .logDirs(logDirInfos)
                        .totalUsedBytes(brokerTotalSize)
                        .totalUsedFormatted(formatBytes(brokerTotalSize))
                        .status(brokerStatus);

                // Add filesystem information if available
                if (!nodeMetrics.isEmpty()) {
                    if (nodeMetrics.containsKey("filesystem_size_bytes")) {
                        long fsSize = Long.parseLong(nodeMetrics.get("filesystem_size_bytes"));
                        builder.filesystemSizeBytes(fsSize)
                               .filesystemSizeFormatted(formatBytes(fsSize));
                    }
                    if (nodeMetrics.containsKey("filesystem_avail_bytes")) {
                        long fsAvail = Long.parseLong(nodeMetrics.get("filesystem_avail_bytes"));
                        builder.filesystemAvailBytes(fsAvail)
                               .filesystemAvailFormatted(formatBytes(fsAvail));
                    }
                    if (nodeMetrics.containsKey("filesystem_used_bytes")) {
                        long fsUsed = Long.parseLong(nodeMetrics.get("filesystem_used_bytes"));
                        builder.filesystemUsedBytes(fsUsed)
                               .filesystemUsedFormatted(formatBytes(fsUsed));
                    }
                    if (nodeMetrics.containsKey("filesystem_used_percent")) {
                        builder.filesystemUsedPercent(Double.parseDouble(nodeMetrics.get("filesystem_used_percent")));
                    }
                    if (nodeMetrics.containsKey("mountpoint")) {
                        builder.mountPoint(nodeMetrics.get("mountpoint"));
                    }
                }

                brokerStorages.add(builder.build());

                totalClusterSize += brokerTotalSize;
            }

            brokerStorages.sort(Comparator.comparingInt(BrokerStorage::getBrokerId));

            return ClusterStorage.builder()
                    .clusterId(clusterId)
                    .totalUsedBytes(totalClusterSize)
                    .totalUsedFormatted(formatBytes(totalClusterSize))
                    .brokerCount(brokerStorages.size())
                    .brokers(brokerStorages)
                    .build();

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("[NMS] Error getting cluster storage: " + e.getMessage());
            return ClusterStorage.builder()
                    .clusterId(clusterId)
                    .error("Error fetching storage info: " + e.getMessage())
                    .brokers(Collections.emptyList())
                    .build();
        }
    }

    private int countBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            return 0;
        }
        return bootstrapServers.split(",").length;
    }

    private List<BootstrapServerStatus> buildBrokerStatuses(String bootstrapServers, Collection<Node> onlineNodes) {
        List<BootstrapServerStatus> statuses = new ArrayList<>();

        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            return statuses;
        }

        String[] servers = bootstrapServers.split(",");

        for (String server : servers) {
            server = server.trim();
            String[] parts = server.split(":");

            if (parts.length != 2) {
                statuses.add(BootstrapServerStatus.builder()
                        .server(server)
                        .host(server)
                        .port(-1)
                        .status("DOWN")
                        .brokerId(null)
                        .build());
                continue;
            }

            String host = parts[0];
            int port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                statuses.add(BootstrapServerStatus.builder()
                        .server(server)
                        .host(host)
                        .port(-1)
                        .status("DOWN")
                        .brokerId(null)
                        .build());
                continue;
            }

            Node matchingNode = null;
            for (Node node : onlineNodes) {
                if (node.host().equals(host) && node.port() == port) {
                    matchingNode = node;
                    break;
                }
                if ((host.equals("localhost") || host.equals("127.0.0.1")) &&
                    (node.host().equals("localhost") || node.host().equals("127.0.0.1")) &&
                    node.port() == port) {
                    matchingNode = node;
                    break;
                }
            }

            if (matchingNode != null) {
                statuses.add(BootstrapServerStatus.builder()
                        .server(server)
                        .host(host)
                        .port(port)
                        .status("UP")
                        .brokerId(matchingNode.id())
                        .build());
            } else {
                statuses.add(BootstrapServerStatus.builder()
                        .server(server)
                        .host(host)
                        .port(port)
                        .status("DOWN")
                        .brokerId(null)
                        .build());
            }
        }

        return statuses;
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else if (bytes < 1024L * 1024 * 1024 * 1024) {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        } else {
            return String.format("%.2f TB", bytes / (1024.0 * 1024 * 1024 * 1024));
        }
    }

    public boolean isReachable() {
        try {
            adminClient.describeCluster().clusterId().get(5, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get system metrics (CPU, Memory) for all brokers.
     */
    public ClusterSystemMetrics getSystemMetrics() {
        List<BrokerSystemMetrics> brokerMetrics = new ArrayList<>();

        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            for (Node node : nodes) {
                int brokerId = node.id();
                String brokerHost = node.host();
                int brokerPort = node.port();

                // Fetch node_exporter metrics
                Map<String, String> nodeMetrics = fetchNodeExporterMetrics(brokerHost);

                BrokerSystemMetrics.BrokerSystemMetricsBuilder builder = BrokerSystemMetrics.builder()
                        .brokerId(brokerId)
                        .host(brokerHost)
                        .port(brokerPort)
                        .status("Online");

                // Add memory information
                if (nodeMetrics.containsKey("memory_total_bytes")) {
                    long memTotal = Long.parseLong(nodeMetrics.get("memory_total_bytes"));
                    builder.memoryTotalBytes(memTotal)
                           .memoryTotalFormatted(formatBytes(memTotal));
                }
                if (nodeMetrics.containsKey("memory_available_bytes")) {
                    long memAvail = Long.parseLong(nodeMetrics.get("memory_available_bytes"));
                    builder.memoryAvailableBytes(memAvail)
                           .memoryAvailableFormatted(formatBytes(memAvail));
                }
                if (nodeMetrics.containsKey("memory_used_bytes")) {
                    long memUsed = Long.parseLong(nodeMetrics.get("memory_used_bytes"));
                    builder.memoryUsedBytes(memUsed)
                           .memoryUsedFormatted(formatBytes(memUsed));
                }
                if (nodeMetrics.containsKey("memory_used_percent")) {
                    builder.memoryUsedPercent(Double.parseDouble(nodeMetrics.get("memory_used_percent")));
                }

                // Add CPU information
                if (nodeMetrics.containsKey("cpu_cores")) {
                    builder.cpuCores(Integer.parseInt(nodeMetrics.get("cpu_cores")));
                }
                if (nodeMetrics.containsKey("cpu_usage_percent")) {
                    builder.cpuUsagePercent(Double.parseDouble(nodeMetrics.get("cpu_usage_percent")));
                }
                if (nodeMetrics.containsKey("load_average_1min")) {
                    builder.loadAverage1min(Double.parseDouble(nodeMetrics.get("load_average_1min")));
                }
                if (nodeMetrics.containsKey("load_average_5min")) {
                    builder.loadAverage5min(Double.parseDouble(nodeMetrics.get("load_average_5min")));
                }
                if (nodeMetrics.containsKey("load_average_15min")) {
                    builder.loadAverage15min(Double.parseDouble(nodeMetrics.get("load_average_15min")));
                }

                brokerMetrics.add(builder.build());
            }

            brokerMetrics.sort(Comparator.comparingInt(BrokerSystemMetrics::getBrokerId));

            return ClusterSystemMetrics.builder()
                    .clusterId(clusterId)
                    .brokerCount(brokerMetrics.size())
                    .brokers(brokerMetrics)
                    .build();

        } catch (Exception e) {
            System.err.println("[NMS] Error getting system metrics: " + e.getMessage());
            return ClusterSystemMetrics.builder()
                    .clusterId(clusterId)
                    .brokerCount(0)
                    .brokers(new ArrayList<>())
                    .error("Error fetching system metrics: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Fetch filesystem metrics from node_exporter running on the broker node.
     */
    private Map<String, String> fetchNodeExporterMetrics(String host) {
        Map<String, String> metrics = new HashMap<>();

        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":19100/metrics"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String body = response.body();

                // Parse Prometheus metrics format (handles scientific notation)
                Pattern sizePattern = Pattern.compile("node_filesystem_size_bytes\\{.*?mountpoint=\"([^\"]+)\".*?}\\s+([\\d.e+]+)");
                Pattern availPattern = Pattern.compile("node_filesystem_avail_bytes\\{.*?mountpoint=\"([^\"]+)\".*?}\\s+([\\d.e+]+)");

                // Memory patterns
                Pattern memTotalPattern = Pattern.compile("node_memory_MemTotal_bytes\\s+([\\d.e+]+)");
                Pattern memAvailPattern = Pattern.compile("node_memory_MemAvailable_bytes\\s+([\\d.e+]+)");

                // Load average patterns
                Pattern load1Pattern = Pattern.compile("node_load1\\s+([\\d.]+)");
                Pattern load5Pattern = Pattern.compile("node_load5\\s+([\\d.]+)");
                Pattern load15Pattern = Pattern.compile("node_load15\\s+([\\d.]+)");

                // CPU cores pattern (count unique cpu IDs)
                Pattern cpuPattern = Pattern.compile("node_cpu_seconds_total\\{cpu=\"(\\d+)\"");

                Matcher sizeMatcher = sizePattern.matcher(body);
                Matcher availMatcher = availPattern.matcher(body);

                // Find metrics for the root filesystem or /var (where Kafka data is typically stored)
                while (sizeMatcher.find()) {
                    String mountpoint = sizeMatcher.group(1);
                    // Look for root or /var mountpoint
                    if (mountpoint.equals("/") || mountpoint.equals("/var") || mountpoint.startsWith("/var/")) {
                        String sizeBytes = sizeMatcher.group(2);
                        // Convert scientific notation to long
                        long size = (long) Double.parseDouble(sizeBytes);
                        metrics.put("filesystem_size_bytes", String.valueOf(size));
                        metrics.put("mountpoint", mountpoint);
                        break; // Use the first matching filesystem
                    }
                }

                while (availMatcher.find()) {
                    String mountpoint = availMatcher.group(1);
                    if (mountpoint.equals("/") || mountpoint.equals("/var") || mountpoint.startsWith("/var/")) {
                        String availBytes = availMatcher.group(2);
                        // Convert scientific notation to long
                        long avail = (long) Double.parseDouble(availBytes);
                        metrics.put("filesystem_avail_bytes", String.valueOf(avail));
                        break;
                    }
                }

                // Calculate used bytes if we have both size and avail
                if (metrics.containsKey("filesystem_size_bytes") && metrics.containsKey("filesystem_avail_bytes")) {
                    long size = Long.parseLong(metrics.get("filesystem_size_bytes"));
                    long avail = Long.parseLong(metrics.get("filesystem_avail_bytes"));
                    long used = size - avail;
                    metrics.put("filesystem_used_bytes", String.valueOf(used));
                    metrics.put("filesystem_used_percent", String.format("%.2f", (used * 100.0) / size));
                }

                // Parse memory metrics
                Matcher memTotalMatcher = memTotalPattern.matcher(body);
                if (memTotalMatcher.find()) {
                    long memTotal = (long) Double.parseDouble(memTotalMatcher.group(1));
                    metrics.put("memory_total_bytes", String.valueOf(memTotal));
                }

                Matcher memAvailMatcher = memAvailPattern.matcher(body);
                if (memAvailMatcher.find()) {
                    long memAvail = (long) Double.parseDouble(memAvailMatcher.group(1));
                    metrics.put("memory_available_bytes", String.valueOf(memAvail));
                }

                // Calculate memory used
                if (metrics.containsKey("memory_total_bytes") && metrics.containsKey("memory_available_bytes")) {
                    long memTotal = Long.parseLong(metrics.get("memory_total_bytes"));
                    long memAvail = Long.parseLong(metrics.get("memory_available_bytes"));
                    long memUsed = memTotal - memAvail;
                    metrics.put("memory_used_bytes", String.valueOf(memUsed));
                    metrics.put("memory_used_percent", String.format("%.2f", (memUsed * 100.0) / memTotal));
                }

                // Parse load averages
                Matcher load1Matcher = load1Pattern.matcher(body);
                if (load1Matcher.find()) {
                    metrics.put("load_average_1min", load1Matcher.group(1));
                }

                Matcher load5Matcher = load5Pattern.matcher(body);
                if (load5Matcher.find()) {
                    metrics.put("load_average_5min", load5Matcher.group(1));
                }

                Matcher load15Matcher = load15Pattern.matcher(body);
                if (load15Matcher.find()) {
                    metrics.put("load_average_15min", load15Matcher.group(1));
                }

                // Count CPU cores
                Matcher cpuMatcher = cpuPattern.matcher(body);
                Set<String> cpuCores = new HashSet<>();
                while (cpuMatcher.find()) {
                    cpuCores.add(cpuMatcher.group(1));
                }
                if (!cpuCores.isEmpty()) {
                    metrics.put("cpu_cores", String.valueOf(cpuCores.size()));
                }

                // Calculate CPU usage percentage from idle time
                Pattern cpuIdlePattern = Pattern.compile("node_cpu_seconds_total\\{cpu=\"(\\d+)\",mode=\"idle\"\\}\\s+([\\d.e+]+)");
                Pattern cpuTotalPattern = Pattern.compile("node_cpu_seconds_total\\{cpu=\"(\\d+)\",mode=\"\\w+\"\\}\\s+([\\d.e+]+)");

                Map<String, Double> cpuIdleTimes = new HashMap<>();
                Map<String, Double> cpuTotalTimes = new HashMap<>();

                Matcher idleMatcher = cpuIdlePattern.matcher(body);
                while (idleMatcher.find()) {
                    String cpuId = idleMatcher.group(1);
                    double idleTime = Double.parseDouble(idleMatcher.group(2));
                    cpuIdleTimes.put(cpuId, idleTime);
                }

                Matcher totalMatcher = cpuTotalPattern.matcher(body);
                while (totalMatcher.find()) {
                    String cpuId = totalMatcher.group(1);
                    double time = Double.parseDouble(totalMatcher.group(2));
                    cpuTotalTimes.put(cpuId, cpuTotalTimes.getOrDefault(cpuId, 0.0) + time);
                }

                // Calculate average CPU usage across all cores
                if (!cpuIdleTimes.isEmpty() && !cpuTotalTimes.isEmpty()) {
                    double totalIdlePercent = 0;
                    int coreCount = 0;
                    for (String cpuId : cpuIdleTimes.keySet()) {
                        if (cpuTotalTimes.containsKey(cpuId)) {
                            double idleTime = cpuIdleTimes.get(cpuId);
                            double totalTime = cpuTotalTimes.get(cpuId);
                            if (totalTime > 0) {
                                double idlePercent = (idleTime / totalTime) * 100.0;
                                totalIdlePercent += idlePercent;
                                coreCount++;
                            }
                        }
                    }
                    if (coreCount > 0) {
                        double avgIdlePercent = totalIdlePercent / coreCount;
                        double cpuUsagePercent = 100.0 - avgIdlePercent;
                        metrics.put("cpu_usage_percent", String.format("%.2f", cpuUsagePercent));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[NMS] Error fetching node_exporter metrics from " + host + ": " + e.getMessage());
        }

        return metrics;
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
}
