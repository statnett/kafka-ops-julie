package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.ALLOW_DELETE_GROUP_CONFIGS;
import static com.purbon.kafka.topology.Constants.TOPOLOGY_TOPIC_STATE_FROM_CLUSTER;
import static org.junit.Assert.*;

import com.purbon.kafka.topology.BackendController;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.ExecutionPlan;
import com.purbon.kafka.topology.GroupConfigManager;
import com.purbon.kafka.topology.TestTopologyBuilder;
import com.purbon.kafka.topology.actions.groups.ResetGroupConfigAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.GroupConfig;
import com.purbon.kafka.topology.model.users.KStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupConfigManagerIT {

  private static SaslPlaintextKafkaContainer container;
  private GroupConfigManager groupConfigManager;
  private AdminClient kafkaAdminClient;

  private ExecutionPlan plan;

  private Topology baseTopology;
  private final String applicationId = "streams-app-a";

  @BeforeClass
  public static void setup() {
    container =
        new SaslPlaintextKafkaContainer()
            .withUser(ContainerTestUtils.PRODUCER_USERNAME)
            .withUser(ContainerTestUtils.CONSUMER_USERNAME)
            .withUser(ContainerTestUtils.BACKUP_USERNAME)
            .withUser("streams-app-a");
    container.start();
  }

  @AfterClass
  public static void tearDown() {
    container.stop();
  }

  @Before
  public void before() throws IOException {
    ContainerTestUtils.clearAclsAndTopics(container);
    kafkaAdminClient = ContainerTestUtils.getSaslJulieAdminClient(container);
    TopologyBuilderAdminClient topologyBuilderAdminClient =
        new TopologyBuilderAdminClient(kafkaAdminClient);
    Properties properties = new Properties();
    properties.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    properties.put(ALLOW_DELETE_GROUP_CONFIGS, "true");
    HashMap<String, String> cliOpts = new HashMap<>();
    cliOpts.put(BROKERS_OPTION, "");
    plan = ExecutionPlan.init(new BackendController(), System.out);
    groupConfigManager =
        new GroupConfigManager(topologyBuilderAdminClient, new Configuration(cliOpts, properties));
    initializeTopology();
  }

  @Test
  public void testIdempotenceOnNoChange() throws IOException {
    // ExecutionPlan.run() never clears the action list (it is an append-only audit log), so we
    // must reset it ourselves before checking that re-running with an unchanged groupConfig
    // schedules no new actions. `initializeTopology()` already applied this exact config, so the
    // broker's current state should already match the declared one.
    plan.getActions().clear();
    groupConfigManager.updatePlan(plan, Map.of("project", baseTopology));
    plan.run();
    Assert.assertEquals(0, plan.getActions().size());
  }

  @Test
  public void testResetGroupConfig() throws IOException, ExecutionException, InterruptedException {
    // Reset actions list
    plan.getActions().clear();

    KStream stream = new KStream();
    stream.setApplicationId(Optional.of(applicationId));
    Topology topology =
        TestTopologyBuilder.createProject("reset-streams-app", "project")
            .addKStream(stream)
            .buildTopology();

    groupConfigManager.updatePlan(plan, Map.of("project", topology));

    plan.run();

    assertTrue(plan.getActions().getFirst() instanceof ResetGroupConfigAction);

    // Once reset, "streams-app-a" is no longer owned/tracked by JulieOps (its ownership record
    // was removed by ResetGroupConfigAction), so no groups should remain in the local state.
    Set<GroupConfig> groups = groupConfigManager.loadClusterState(plan);
    assertEquals(0, groups.size());

    // Verify the actual broker-side effect of the reset: all four properties should have
    // fallen back to their broker default values.
    ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, "streams-app-a");
    Map<ConfigResource, Config> result =
        kafkaAdminClient
            .describeConfigs(
                Collections.singleton(
                    new ConfigResource(ConfigResource.Type.GROUP, "streams-app-a")),
                new DescribeConfigsOptions())
            .all()
            .get();
    assertTrue(result.containsKey(groupResource));
    Config config = result.get(groupResource);
    assertTrue(config.get("streams.session.timeout.ms").isDefault());
    assertEquals("45000", config.get("streams.session.timeout.ms").value());
    assertTrue(config.get("streams.heartbeat.interval.ms").isDefault());
    assertEquals("5000", config.get("streams.heartbeat.interval.ms").value());
    assertTrue(config.get("streams.num.standby.replicas").isDefault());
    assertEquals("0", config.get("streams.num.standby.replicas").value());
    assertTrue(config.get("streams.initial.rebalance.delay.ms").isDefault());
    assertEquals("3000", config.get("streams.initial.rebalance.delay.ms").value());
  }

  @Test
  public void testModifiedGroupConfig()
      throws IOException, ExecutionException, InterruptedException {
    // Reset actions list
    plan.getActions().clear();
    KStream stream = baseTopology.getProjects().getFirst().getStreams().getFirst();
    // Simulate a reset where all fields are removed, except one which is modified from the original
    //noinspection OptionalGetWithoutIsPresent
    GroupConfig groupConfig = stream.getGroupConfig().get();
    groupConfig.setNumStandbyReplicas(Optional.of(2));
    groupConfig.setSessionTimeoutMs(Optional.empty());
    groupConfig.setHeartbeatIntervalMs(Optional.empty());
    groupConfig.setInitialRebalanceDelayMs(Optional.empty());
    stream.setGroupConfig(Optional.of(groupConfig));
    baseTopology.getProjects().getFirst().setStreams(List.of(stream));

    groupConfigManager.updatePlan(plan, Map.of("project", baseTopology));
    plan.run();

    // Verify against the actual broker state, rather than `loadClusterState(plan)`: with
    // TOPOLOGY_TOPIC_STATE_FROM_CLUSTER disabled, that method only tracks group IDs locally and
    // can never carry real field values, so it cannot be used to assert on config content here.
    ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, applicationId);
    Map<ConfigResource, Config> result =
        kafkaAdminClient
            .describeConfigs(Collections.singleton(groupResource), new DescribeConfigsOptions())
            .all()
            .get();
    Config config = result.get(groupResource);

    // The modified field must carry it's new, non-default value.
    assertFalse(config.get("streams.num.standby.replicas").isDefault());
    assertEquals("2", config.get("streams.num.standby.replicas").value());

    // The removed fields must have fallen back to the broker default.
    assertTrue(config.get("streams.session.timeout.ms").isDefault());
    assertTrue(config.get("streams.heartbeat.interval.ms").isDefault());
    assertTrue(config.get("streams.initial.rebalance.delay.ms").isDefault());
  }

  private void initializeTopology() throws IOException {
    baseTopology = new TopologyImpl();
    baseTopology.setContext("reset-streams-app");

    Project baseProject = new ProjectImpl("project");

    Map<String, List<String>> topics = new HashMap<>();
    List<User> observerPrincipals = new ArrayList<>();
    GroupConfig groupConfig = new GroupConfig();
    groupConfig.setGroupId(applicationId);
    groupConfig.setHeartbeatIntervalMs(Optional.of(6500));
    groupConfig.setInitialRebalanceDelayMs(Optional.of(4000));
    groupConfig.setSessionTimeoutMs(Optional.of(50000));
    groupConfig.setNumStandbyReplicas(Optional.of(1));

    KStream baseStream =
        new KStream(
            "streams-app-a",
            topics,
            observerPrincipals,
            Optional.of(applicationId),
            Optional.of(true),
            Optional.of(groupConfig));
    baseProject.setStreams(List.of(baseStream));
    baseTopology.addProject(baseProject);
    groupConfigManager.updatePlan(plan, Map.of(baseProject.getName(), baseTopology));
    plan.run();

    KStream observedStream = baseTopology.getProjects().getFirst().getStreams().getFirst();
    Assert.assertNotNull(observedStream);

    Assert.assertTrue(observedStream.getApplicationId().isPresent());
    Assert.assertTrue(observedStream.getGroupConfig().isPresent());

    Assert.assertTrue(baseStream.getApplicationId().isPresent());
    final GroupConfig observedGroupConfig = observedStream.getGroupConfig().get();

    Assert.assertEquals(baseStream.getApplicationId().get(), observedGroupConfig.getGroupId());

    Assert.assertTrue(groupConfig.getHeartbeatIntervalMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getHeartbeatIntervalMs().isPresent());
    Assert.assertEquals(
        groupConfig.getHeartbeatIntervalMs().get(),
        observedGroupConfig.getHeartbeatIntervalMs().get());

    Assert.assertTrue(groupConfig.getSessionTimeoutMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getSessionTimeoutMs().isPresent());
    Assert.assertEquals(
        groupConfig.getSessionTimeoutMs().get(), observedGroupConfig.getSessionTimeoutMs().get());

    Assert.assertTrue(groupConfig.getNumStandbyReplicas().isPresent());
    Assert.assertTrue(observedGroupConfig.getNumStandbyReplicas().isPresent());
    Assert.assertEquals(
        groupConfig.getNumStandbyReplicas().get(),
        observedGroupConfig.getNumStandbyReplicas().get());

    Assert.assertTrue(groupConfig.getInitialRebalanceDelayMs().isPresent());
    Assert.assertTrue(observedGroupConfig.getInitialRebalanceDelayMs().isPresent());
    Assert.assertEquals(
        groupConfig.getInitialRebalanceDelayMs().get(),
        observedGroupConfig.getInitialRebalanceDelayMs().get());
  }
}
