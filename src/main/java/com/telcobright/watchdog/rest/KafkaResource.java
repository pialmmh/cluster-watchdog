package com.telcobright.watchdog.rest;

import com.telcobright.watchdog.config.ClusterConfig;
import com.telcobright.watchdog.model.ClusterHealth;
import com.telcobright.watchdog.service.DockerMonitor;
import com.telcobright.watchdog.service.KafkaMonitor;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Path("/api/kafka")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaResource {

    @Inject
    KafkaMonitor kafkaMonitor;

    @Inject
    DockerMonitor dockerMonitor;

    @Inject
    ClusterConfig config;

    @GET
    public Map<String, Object> getClusterStatus() {
        String bootstrap = config.kafka().bootstrapServers();
        ClusterHealth.KafkaClusterStatus status = kafkaMonitor.checkCluster(bootstrap);

        return Map.of(
                "bootstrapServers", bootstrap,
                "status", status,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/brokers")
    public Map<String, Object> getBrokerStatus() {
        String bootstrap = config.kafka().bootstrapServers();
        String[] servers = bootstrap.split(",");

        var brokerStatus = new java.util.ArrayList<Map<String, Object>>();
        for (String server : servers) {
            boolean reachable = kafkaMonitor.isBrokerReachable(server.trim());
            brokerStatus.add(Map.of(
                    "address", server.trim(),
                    "reachable", reachable
            ));
        }

        return Map.of(
                "brokers", brokerStatus,
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/topics")
    public Map<String, Object> listTopics() {
        String bootstrap = config.kafka().bootstrapServers();
        List<String> topics = kafkaMonitor.listTopics(bootstrap);

        return Map.of(
                "topics", topics,
                "count", topics.size(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/topics/{name}")
    public Map<String, Object> getTopicInfo(@PathParam("name") String topicName) {
        String bootstrap = config.kafka().bootstrapServers();
        var topicInfo = kafkaMonitor.describeTopic(bootstrap, topicName);

        if (topicInfo == null) {
            return Map.of(
                    "topic", topicName,
                    "error", "Topic not found or not accessible",
                    "checkedAt", Instant.now().toString()
            );
        }

        return Map.of(
                "topic", topicName,
                "partitions", topicInfo.partitions(),
                "replicationFactor", topicInfo.replicationFactor(),
                "checkedAt", Instant.now().toString()
        );
    }

    @GET
    @Path("/containers")
    public Map<String, Object> getKafkaContainers() {
        var allContainers = dockerMonitor.listContainers();
        var kafkaContainers = allContainers.stream()
                .filter(c -> c.name().toLowerCase().contains("kafka"))
                .toList();

        return Map.of(
                "containers", kafkaContainers,
                "count", kafkaContainers.size(),
                "checkedAt", Instant.now().toString()
        );
    }
}
