package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
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
    HashMap<String, String> cliOpts = new HashMap<>();
    cliOpts.put(BROKERS_OPTION, "");
    plan = ExecutionPlan.init(new BackendController(), System.out);
    groupConfigManager =
        new GroupConfigManager(topologyBuilderAdminClient, new Configuration(cliOpts, properties));
    initializeTopology();
  }

  @Test
  public void testIdempotenceOnNoChange() throws IOException {
    // Do not reset actions list, run same again
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

    // Should be one existing group after executing plan
    Set<GroupConfig> groups = groupConfigManager.loadClusterState(plan);
    assertEquals(1, groups.size());

    // NOTE: when fetching state from `plan`, the resulting objects contain empty fields, causing
    // these assertions to fail
    //    GroupConfig observedResetStreamGroupConfig = groups.stream().toList().getFirst();
    //    Assert.assertTrue(observedResetStreamGroupConfig.getSessionTimeoutMs().isPresent());
    //    final int resetSessionTimeoutMs =
    // observedResetStreamGroupConfig.getSessionTimeoutMs().get();
    //    Assert.assertEquals(60000, resetSessionTimeoutMs);
    //    Assert.assertTrue(observedResetStreamGroupConfig.getHeartbeatIntervalMs().isPresent());
    //    final int resetHeartbeatIntervalMs =
    //        observedResetStreamGroupConfig.getHeartbeatIntervalMs().get();
    //    Assert.assertEquals(5000, resetHeartbeatIntervalMs);
    //    Assert.assertTrue(observedResetStreamGroupConfig.getNumStandbyReplicas().isPresent());
    //    final int resetNumStandbyReplicas =
    //        observedResetStreamGroupConfig.getNumStandbyReplicas().get();
    //    Assert.assertEquals(0, resetNumStandbyReplicas);
    //
    // Assert.assertTrue(observedResetStreamGroupConfig.getInitialRebalanceDelayMs().isPresent());
    //    final int resetInitialRebalanceDelayMs =
    //        observedResetStreamGroupConfig.getInitialRebalanceDelayMs().get();
    //    Assert.assertEquals(3000, resetInitialRebalanceDelayMs);

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
  public void testModifiedGroupConfig() throws IOException {
    // Reset actions list
    plan.getActions().clear();
    KStream stream = baseTopology.getProjects().getFirst().getStreams().getFirst();
    // Simulate a reset where all fields are removed, except one which is modified from the original
    GroupConfig groupConfig = stream.getGroupConfig().get();
    groupConfig.setNumStandbyReplicas(Optional.of(2));
    groupConfig.setSessionTimeoutMs(Optional.empty());
    groupConfig.setHeartbeatIntervalMs(Optional.empty());
    groupConfig.setInitialRebalanceDelayMs(Optional.empty());
    stream.setGroupConfig(Optional.of(groupConfig));
    baseTopology.getProjects().getFirst().setStreams(List.of(stream));

    groupConfigManager.updatePlan(plan, Map.of("project", baseTopology));
    plan.run();

    Set<GroupConfig> groups = groupConfigManager.loadClusterState(plan);
    GroupConfig observedResetStreamGroupConfig = groups.stream().toList().getFirst();

    Assert.assertTrue(observedResetStreamGroupConfig.getNumStandbyReplicas().isPresent());
    final int resetNumStandbyReplicas =
        observedResetStreamGroupConfig.getNumStandbyReplicas().get();
    Assert.assertEquals(2, resetNumStandbyReplicas);
    Assert.assertTrue(observedResetStreamGroupConfig.getInitialRebalanceDelayMs().isPresent());
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
