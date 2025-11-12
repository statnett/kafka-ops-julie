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

  private Role role;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> status_topic;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> offset_topic;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Optional<String> configs_topic;

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

  public Optional<String> getStatusTopic() {
    return status_topic;
  }

  public void setStatus_topic(Optional<String> status_topic) {
    this.status_topic = status_topic;
  }

  public Optional<String> getOffset_topic() {
    return offset_topic;
  }

  public void setOffset_topic(Optional<String> offset_topic) {
    this.offset_topic = offset_topic;
  }

  public Optional<String> getConfigs_topic() {
    return configs_topic;
  }

  public void setConfigs_topic(Optional<String> configs_topic) {
    this.configs_topic = configs_topic;
  }

  public Optional<String> getTarget_prefix() {
    return target_prefix;
  }

  public void setTarget_prefix(Optional<String> target_prefix) {
    this.target_prefix = target_prefix;
  }

  public Optional<String> getOffset_syncs_topic() {
    return offset_syncs_topic;
  }

  public void setOffset_syncs_topic(Optional<String> offset_syncs_topic) {
    this.offset_syncs_topic = offset_syncs_topic;
  }

  public Optional<String> getCheckpoints_topic() {
    return checkpoints_topic;
  }

  public void setCheckpoints_topic(Optional<String> checkpoints_topic) {
    this.checkpoints_topic = checkpoints_topic;
  }

  public Optional<String> getHeartbeats_topic() {
    return heartbeats_topic;
  }

  public void setHeartbeats_topic(Optional<String> heartbeats_topic) {
    this.heartbeats_topic = heartbeats_topic;
  }

  public Optional<String> getGroup() {
    return group;
  }

  public void setGroup(Optional<String> group) {
    this.group = group;
  }

  public List<Optional<String>> getSource_topics() {
    return source_topics;
  }

  public void setSource_topics(List<Optional<String>> source_topics) {
    this.source_topics = source_topics;
  }

  public List<Optional<String>> getTarget_topics() {
    return target_topics;
  }

  public void setTargetTopics(List<Optional<String>> target_topics) {
    this.target_topics = target_topics;
  }

  public List<String> getAllTopics() {
    List<String> allTopics = new ArrayList<>();

    if (this.role == Role.consumer) {
      this.source_topics.forEach(
          t -> {
            allTopics.add(t.get());
          });
    }

    if (this.role == Role.producer) {
      this.target_topics.forEach(
          t -> {
            allTopics.add(t.get());
          });
      allTopics.add(this.offset_syncs_topic.get());
      this.checkpoints_topic.ifPresent(allTopics::add);
      this.heartbeats_topic.ifPresent(allTopics::add);
    }

    return allTopics;
  }
}
