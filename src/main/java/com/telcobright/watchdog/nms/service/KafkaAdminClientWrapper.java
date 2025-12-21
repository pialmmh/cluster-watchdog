package com.telcobright.watchdog.nms.service;

import com.telcobright.watchdog.nms.dto.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    public ClusterOverview getClusterOverview(String tenantName) {
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
                    .clusterId(kafkaClusterId)
                    .clusterStatus(status)
                    .totalBrokers(totalBrokers)
                    .onlineBrokers(onlineBrokers)
                    .upBrokers(onlineBrokers)
                    .downBrokers(downBrokers)
                    .controllerId(controller != null ? controller.id() : -1)
                    .tenantName(tenantName)
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

                brokerStorages.add(BrokerStorage.builder()
                        .brokerId(brokerId)
                        .host(brokerNode != null ? brokerNode.host() : "unknown")
                        .port(brokerNode != null ? brokerNode.port() : -1)
                        .storagePaths(storagePaths)
                        .logDirs(logDirInfos)
                        .totalUsedBytes(brokerTotalSize)
                        .totalUsedFormatted(formatBytes(brokerTotalSize))
                        .status(brokerStatus)
                        .build());

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

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
}
