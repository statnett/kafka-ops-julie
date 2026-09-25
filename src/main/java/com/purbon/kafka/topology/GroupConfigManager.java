package com.purbon.kafka.topology;

import com.purbon.kafka.topology.actions.Action;
import com.purbon.kafka.topology.actions.groups.ResetGroupConfigAction;
import com.purbon.kafka.topology.actions.groups.UpdateGroupConfigAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.GroupConfig;
import com.purbon.kafka.topology.model.users.KStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupConfigManager implements ExecutionPlanUpdater {

  private final TopologyBuilderAdminClient adminClient;
  private final Configuration config;

  public GroupConfigManager(TopologyBuilderAdminClient adminClient, Configuration config) {
    this.adminClient = adminClient;
    this.config = config;
  }

  @Override
  public void updatePlan(ExecutionPlan plan, Map<String, Topology> topologies) throws IOException {
    for (Map.Entry<String, Topology> entry : topologies.entrySet()) {
      Topology topology = entry.getValue();
//      Set<String> existingGroupIDs =
//          loadClusterState(plan).stream().map(GroupConfig::getGroupId).collect(Collectors.toSet());
      Set<Action> createGroups = new LinkedHashSet<>();
      Set<String> groupsToDelete = new LinkedHashSet<>();
      Set<List<KStream>> streams = topology
              .getProjects()
              .stream()
              .map(Project::getStreams)
              .collect(Collectors.toSet());
      streams.forEach(kStreams -> kStreams.forEach(kStream -> {
          // NOTE: internal testing showed that not all streams applications use a 1:1
          // relationship between application ID and group ID. For now, require users to
          // update applicationID for every group update, and in the future consider
          // adding an optional `groupId` field with fallback value to `applicationId`
          final String applicationId = kStream.getApplicationId().orElseThrow();
          if (kStream.getGroupConfig().isPresent()) {
              createGroups.add(
                      new UpdateGroupConfigAction(
                              this.adminClient, kStream.getGroupConfig().get()));
          } else {
              groupsToDelete.add(applicationId);
          }
      }));
      if (!createGroups.isEmpty()) {
        createGroups.forEach(plan::add);
      }
      if(!groupsToDelete.isEmpty()) {
          plan.add(new ResetGroupConfigAction(this.adminClient, groupsToDelete.stream().toList()));
      }
    }
  }

  //  public Set<String> loadClusterState(final ExecutionPlan plan) {
  //    if (config.fetchStateFromTheCluster()) {
  //      return this.adminClient.listGroups();
  //    }
  //    return plan.getStreamGroups();
  //  }

  public Set<GroupConfig> loadClusterState(final ExecutionPlan plan) {
    if (config.fetchStateFromTheCluster()) {
      return this.adminClient.describeGroups();
    }
    Set<GroupConfig> groupConfigs = new LinkedHashSet<>();
    plan.getStreamGroups()
        .forEach(
            group -> {
              GroupConfig groupConfig = new GroupConfig(group);
              groupConfigs.add(groupConfig);
            });
    return groupConfigs;
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of groups");
    out.println(this.adminClient.listGroups());
  }
}
