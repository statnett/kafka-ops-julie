package com.purbon.kafka.topology.model.users;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.User;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MirrorMaker2 extends DynamicUser {

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

  public MirrorMaker2(
      String principal,
      Map<String, List<String>> topics,
      List<User> observerPrincipals,
      Optional<String> status_topic,
      Optional<String> offset_topic,
      Optional<String> configs_topic,
      Optional<String> targetPrefix,
      Optional<String> offset_syncs_topic,
      Optional<String> checkpoints_topic,
      Optional<String> heartbeats_topic) {
    super(principal, topics, observerPrincipals);
    this.status_topic = status_topic;
    this.offset_topic = offset_topic;
    this.configs_topic = configs_topic;
    this.target_prefix = targetPrefix;
    this.offset_syncs_topic = offset_syncs_topic;
    this.checkpoints_topic = checkpoints_topic;
    this.heartbeats_topic = heartbeats_topic;
  }
}
