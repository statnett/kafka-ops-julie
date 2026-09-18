package com.purbon.kafka.topology.model.users;

import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class GroupConfig {

  private String groupId;
  private Optional<Integer> sessionTimeoutMs;
  private Optional<Integer> heartbeatIntervalMs;
  private Optional<Integer> numStandbyReplicas;
  private Optional<Integer> initialRebalanceDelayMs;

  public GroupConfig() {
    this.sessionTimeoutMs = Optional.empty();
    this.heartbeatIntervalMs = Optional.empty();
    this.numStandbyReplicas = Optional.empty();
    this.initialRebalanceDelayMs = Optional.empty();
  }

  public GroupConfig(final String groupId) {
    this.groupId = groupId;
    this.sessionTimeoutMs = Optional.empty();
    this.heartbeatIntervalMs = Optional.empty();
    this.numStandbyReplicas = Optional.empty();
    this.initialRebalanceDelayMs = Optional.empty();
  }
}
