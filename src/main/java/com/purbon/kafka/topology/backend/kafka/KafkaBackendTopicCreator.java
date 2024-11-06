package com.purbon.kafka.topology.backend.kafka;

import com.purbon.kafka.topology.Configuration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaBackendTopicCreator {

  private static final Logger LOGGER = LogManager.getLogger(KafkaBackendTopicCreator.class);

  private final Configuration config;
  private Admin admin;

  public KafkaBackendTopicCreator(Configuration config) {
    this.config = config;
  }

  public void configure() {
    admin = AdminClient.create(config.asProperties());
  }

  public boolean createStateTopicUnlessPresent() throws ExecutionException, InterruptedException {
    if (stateTopicExists()) {
      return false;
    }
    if (config.isDryRun()) {
      LOGGER.info("Dry-run: not creating state topic " + config.getKafkaBackendStateTopic());
      return true;
    }
    LOGGER.info("Creating state topic " + config.getKafkaBackendStateTopic());
    Map<String, String> topicConfig = new HashMap<>();
    topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    NewTopic configTopic =
        new NewTopic(config.getKafkaBackendStateTopic(), Math.min(3, getNumBrokers()), (short) 1);
    admin.createTopics(Collections.singleton(configTopic)).all().get();
    return true;
  }

  private boolean stateTopicExists() {
    try {
      return admin.listTopics().names().get().stream()
          .anyMatch(topicName -> topicName.equalsIgnoreCase(config.getKafkaBackendStateTopic()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int getNumBrokers() {
    DescribeClusterResult describeClusterResult = admin.describeCluster();
    try {
      Collection<Node> nodes = describeClusterResult.nodes().get();
      return nodes.size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
