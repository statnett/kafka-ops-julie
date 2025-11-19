package com.purbon.kafka.topology.actions.access.builders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsBuilder;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsResult;
import com.purbon.kafka.topology.model.Component;
import com.purbon.kafka.topology.model.User;

public class ClusterLevelAclBindingsBuilder implements AclBindingsBuilder {

  private final String role;
  private final User user;
  private final Component cmp;
  private final BindingsBuilderProvider builderProvider;

  public ClusterLevelAclBindingsBuilder(
      BindingsBuilderProvider builderProvider, String role, User user, Component cmp) {
    this.builderProvider = builderProvider;
    this.role = role;
    this.user = user;
    this.cmp = cmp;
  }

  @Override
  public AclBindingsResult getAclBindings() {
    return AclBindingsResult.forAclBindings(
        builderProvider.setClusterLevelRole(role, user.getPrincipal(), cmp));
  }
}
