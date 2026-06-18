package com.purbon.kafka.topology.model.users;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.User;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MirrorMaker2 extends DynamicUser {

  public enum Role {
    consumer,
    producer
  };

  private static final String DEFAULT_CONNECT_STATUS_TOPIC = "connect-status";
  private static final String DEFAULT_CONNECT_OFFSET_TOPIC = "connect-offsets";
  private static final String DEFAULT_CONNECT_CONFIGS_TOPIC = "connect-configs";
  private static final String DEFAULT_CONNECT_GROUP = "connect-cluster";
  private static final String DEFAULT_MM2_OFFSET_SYNCS_TOPIC = "mm2.offset_syncs";
  private static final String DEFAULT_MM2_HEARTBEATS_TOPIC = "mm2.heartbeats";
  private static final String DEFAULT_MM2_CHECKPOINTS_TOPIC = "mm2.checkpoints";

  private Role role;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> status_topic;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> offset_topic;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> configs_topic;

  // TODO: need to be prepended to internal topics
  private Optional<String> target_prefix;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> offset_syncs_topic;

  private Optional<String> checkpoints_topic;

  private Optional<String> heartbeats_topic;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> group;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<Optional<String>> source_topics;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<Optional<String>> target_topics;

  public MirrorMaker2(
      Role role,
      String principal,
      Map<String, List<String>> topics,
      List<User> observerPrincipals,
      Optional<String> status_topic,
      Optional<String> offset_topic,
      Optional<String> configs_topic,
      Optional<String> targetPrefix,
      Optional<String> offset_syncs_topic,
      Optional<String> checkpoints_topic,
      Optional<String> heartbeats_topic,
      List<Optional<String>> source_topics,
      List<Optional<String>> target_topics) {
    super(principal, topics, observerPrincipals);
    this.role = role;
    this.status_topic = status_topic;
    this.offset_topic = offset_topic;
    this.configs_topic = configs_topic;
    this.target_prefix = targetPrefix;
    this.offset_syncs_topic = offset_syncs_topic;
    this.checkpoints_topic = checkpoints_topic;
    this.heartbeats_topic = heartbeats_topic;
    this.source_topics = source_topics;
    this.target_topics = target_topics;
  }

  public Role getRole() {
    return role;
  }

  public void setRole(Role role) {
    this.role = role;
  }

  public String statusTopicString() {
    return status_topic.orElse(DEFAULT_CONNECT_STATUS_TOPIC);
  }

  public String offsetTopicString() {
    return offset_topic.orElse(DEFAULT_CONNECT_OFFSET_TOPIC);
  }

  public String configsTopicString() {
    return configs_topic.orElse(DEFAULT_CONNECT_CONFIGS_TOPIC);
  }

  public String groupString() {
    return group.orElse(DEFAULT_CONNECT_GROUP);
  }

  public void setStatusTopic(Optional<String> status_topic) {
    this.status_topic = status_topic;
  }

  public void setOffsetTopic(Optional<String> offset_topic) {
    this.offset_topic = offset_topic;
  }

  public void setConfigsTopic(Optional<String> configs_topic) {
    this.configs_topic = configs_topic;
  }

  public Optional<String> getOffsetSyncsTopic() {
    return this.offset_syncs_topic;
  }

  public Optional<String> getCheckpointsTopic() {
    return this.checkpoints_topic;
  }

  public Optional<String> getHeartbeatsTopic() {
    return this.heartbeats_topic;
  }

  public void setGroup(Optional<String> group) {
    this.group = group;
  }

  public String offsetSyncsTopicString() {
    return offset_syncs_topic.orElse(DEFAULT_MM2_OFFSET_SYNCS_TOPIC);
  }

  public String checkpointsTopicString() {
    return checkpoints_topic.orElse(DEFAULT_MM2_CHECKPOINTS_TOPIC);
  }

  public String heartbeatsTopicString() {
    return heartbeats_topic.orElse(DEFAULT_MM2_HEARTBEATS_TOPIC);
  }

  public List<Optional<String>> getSourceTopics() {
    return source_topics;
  }

  public void setSourceTopics(List<Optional<String>> source_topics) {
    this.source_topics = source_topics;
  }

  public List<Optional<String>> getTargetTopics() {
    return target_topics;
  }

  public void setTargetTopics(List<Optional<String>> target_topics) {
    this.target_topics = target_topics;
  }

  public List<String> getAllTopics() {
    List<String> allTopics =
        new ArrayList<>(
            this.source_topics.stream().filter(Optional::isPresent).map(Optional::get).toList());

    allTopics.addAll(
        this.target_topics.stream().filter(Optional::isPresent).map(Optional::get).toList());

    allTopics.add(this.offsetSyncsTopicString());

    this.checkpoints_topic.ifPresent(allTopics::add);
    this.heartbeats_topic.ifPresent(allTopics::add);

    return allTopics;
  }
}
