package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.Constants.ALLOW_DELETE_GROUP_CONFIGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.purbon.kafka.topology.actions.Action;
import com.purbon.kafka.topology.actions.groups.ResetGroupConfigAction;
import com.purbon.kafka.topology.actions.groups.UpdateGroupConfigAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.GroupConfig;
import com.purbon.kafka.topology.model.users.KStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class GroupConfigManagerTest {

  @Mock TopologyBuilderAdminClient adminClient;

  @Mock PrintStream outputStream;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private ExecutionPlan plan;
  private HashMap<String, String> cliOps;
  private Properties props;

  @Before
  public void setup() throws IOException {
    Files.deleteIfExists(Paths.get(".cluster-state"));
    final BackendController backendController = new BackendController();
    cliOps = new HashMap<>();
    props = new Properties();
    plan = ExecutionPlan.init(backendController, outputStream);
  }

  private GroupConfigManager buildManager(boolean allowDeleteGroupConfigs) {
    props.put(ALLOW_DELETE_GROUP_CONFIGS, String.valueOf(allowDeleteGroupConfigs));
    Configuration config = new Configuration(cliOps, props);
    return new GroupConfigManager(adminClient, config);
  }

  private Topology topologyWithStream(KStream kStream) {
    Topology topology = new TopologyImpl();
    topology.setContext("context");
    Project project = new ProjectImpl("project");
    project.setStreams(Collections.singletonList(kStream));
    topology.setProjects(Collections.singletonList(project));
    return topology;
  }

  private KStream kStreamWithGroupConfig(GroupConfig groupConfig) {
    KStream kStream = new KStream();
    kStream.setApplicationId(Optional.of("app-a"));
    kStream.setGroupConfig(Optional.ofNullable(groupConfig));
    return kStream;
  }

  @Test
  public void shouldScheduleUpdateForStreamWithDeclaredGroupConfig() throws IOException {
    GroupConfig groupConfig = new GroupConfig();
    groupConfig.setGroupId("app-a");
    groupConfig.setNumStandbyReplicas(Optional.of(2));
    KStream kStream = kStreamWithGroupConfig(groupConfig);
    Topology topology = topologyWithStream(kStream);

    GroupConfigManager manager = buildManager(true);
    manager.updatePlan(plan, Map.of("project", topology));

    List<Action> actions = plan.getActions();
    assertEquals(1, actions.size());
    assertTrue(actions.getFirst() instanceof UpdateGroupConfigAction);
  }

  @Test
  public void shouldNotResetGroupNeverManagedByJulieOps() throws IOException {
    // "app-a" has no groupConfig block and was never tracked as JulieOps-managed before, so
    // it must be left alone even though delete is allowed.
    KStream kStream = kStreamWithGroupConfig(null);
    Topology topology = topologyWithStream(kStream);

    GroupConfigManager manager = buildManager(true);
    manager.updatePlan(plan, Map.of("project", topology));

    assertTrue(plan.getActions().isEmpty());
  }

  @Test
  public void shouldResetPreviouslyManagedGroupWhenGroupConfigBlockIsRemoved() throws IOException {
    // Simulate a prior successful apply that put "app-a" under JulieOps management.
    GroupConfig groupConfig = new GroupConfig();
    groupConfig.setGroupId("app-a");
    plan.add(new UpdateGroupConfigAction(adminClient, groupConfig));
    plan.run();

    BackendController reloadedBackend = new BackendController();
    ExecutionPlan newPlan = ExecutionPlan.init(reloadedBackend, outputStream);

    KStream kStream = kStreamWithGroupConfig(null);
    Topology topology = topologyWithStream(kStream);

    GroupConfigManager manager = buildManager(true);
    manager.updatePlan(newPlan, Map.of("project", topology));

    List<Action> actions = newPlan.getActions();
    assertEquals(1, actions.size());
    assertTrue(actions.getFirst() instanceof ResetGroupConfigAction);
    assertEquals(
        List.of("app-a"), ((ResetGroupConfigAction) actions.getFirst()).getGroupsToReset());
  }

  @Test
  public void shouldNotResetPreviouslyManagedGroupWhenDeleteIsNotAllowed() throws IOException {
    GroupConfig groupConfig = new GroupConfig();
    groupConfig.setGroupId("app-a");
    plan.add(new UpdateGroupConfigAction(adminClient, groupConfig));
    plan.run();

    BackendController reloadedBackend = new BackendController();
    ExecutionPlan newPlan = ExecutionPlan.init(reloadedBackend, outputStream);

    KStream kStream = kStreamWithGroupConfig(null);
    Topology topology = topologyWithStream(kStream);

    GroupConfigManager manager = buildManager(false);
    manager.updatePlan(newPlan, Map.of("project", topology));

    assertTrue(newPlan.getActions().isEmpty());
  }
}
