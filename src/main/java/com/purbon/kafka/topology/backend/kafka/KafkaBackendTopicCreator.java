package com.purbon.kafka.topology.backend.kafka;

import com.purbon.kafka.topology.Configuration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
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

  public boolean createStateTopicUnlessPresent() {
    if (stateTopicExists()) {
      assertExistingTopicIsCompacting();
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
        new NewTopic(config.getKafkaBackendStateTopic(), 1, (short) Math.min(3, getNumBrokers()))
            .configs(topicConfig);
    try {
      admin.createTopics(Collections.singleton(configTopic)).all().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  private void assertExistingTopicIsCompacting() {
    try {
      for (Config config :
          admin
              .describeConfigs(
                  Collections.singleton(
                      new ConfigResource(
                          ConfigResource.Type.TOPIC, config.getKafkaBackendStateTopic())))
              .all()
              .get()
              .values()) {
        ConfigEntry configEntry = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
        if (configEntry != null
            && configEntry.value() != null
            && configEntry.value().contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
          return;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException(
        "Topic "
            + config.getKafkaBackendStateTopic()
            + " must have cleanup policy "
            + TopicConfig.CLEANUP_POLICY_COMPACT);
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
