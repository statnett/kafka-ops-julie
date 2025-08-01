package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.junit.Assert.*;

import com.purbon.kafka.topology.*;
import com.purbon.kafka.topology.actions.Action;
import com.purbon.kafka.topology.actions.quotas.CreateQuotasAction;
import com.purbon.kafka.topology.actions.quotas.DeleteQuotasAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.integration.containerutils.*;
import com.purbon.kafka.topology.model.*;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.Producer;
import com.purbon.kafka.topology.model.users.Quota;
import com.purbon.kafka.topology.model.users.platform.Kafka;
import com.purbon.kafka.topology.quotas.QuotasManager;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.roles.acls.AclsBindingsBuilder;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.junit.*;
import org.junit.runner.OrderWith;
import org.junit.runner.manipulation.Alphanumeric;

@OrderWith(Alphanumeric.class)
public class QuotasManagerIT {

  private static SaslPlaintextKafkaContainer container;
  private static AdminClient kafkaAdminClient;

  private TopologyBuilderAdminClient topologyAdminClient;
  private AccessControlManager accessControlManager;
  private SimpleAclsProvider aclsProvider;

  private ExecutionPlan plan;
  private BackendController cs;

  private Configuration config;

  private QuotasManager quotasManager;
  private AclsBindingsBuilder bindingsBuilder;

  @BeforeClass
  public static void setup() {
    container =
        ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"))
            .withUser("user1")
            .withUser("user2")
            .withUser("user3");
    container.start();
  }

  @AfterClass
  public static void teardown() {
    container.stop();
  }

  @Before
  public void before() throws IOException {
    kafkaAdminClient = ContainerTestUtils.getSaslJulieAdminClient(container);
    topologyAdminClient = new TopologyBuilderAdminClient(kafkaAdminClient);
    ContainerTestUtils.resetAcls(container);
    TestUtils.deleteStateFile();

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);
    props.put(ALLOW_DELETE_BINDINGS, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CCLOUD_ENV_CONFIG, "");

    config = new Configuration(cliOps, props);

    this.cs = new BackendController();
    this.plan = ExecutionPlan.init(cs, System.out);

    bindingsBuilder = new AclsBindingsBuilder(config);
    quotasManager = new QuotasManager(topologyAdminClient, config);
    aclsProvider = new SimpleAclsProvider(topologyAdminClient);

    accessControlManager = new AccessControlManager(aclsProvider, bindingsBuilder, config);
  }

  private Topology woldMSpecPattern() {
    List<Producer> producers = new ArrayList<>();
    Producer producer = new Producer("User:user1");
    producers.add(producer);
    Producer producer2 = new Producer("User:user2");
    producers.add(producer2);

    List<Consumer> consumers = new ArrayList<>();
    Consumer consumer = new Consumer("User:user1");
    consumers.add(consumer);
    Consumer consumer2 = new Consumer("User:user2");
    consumers.add(consumer2);

    Project project = new ProjectImpl("project");
    project.setProducers(producers);
    project.setConsumers(consumers);

    Topic topicA = new Topic("topicA");
    project.addTopic(topicA);

    Topology topology = new TopologyImpl();
    topology.setContext("integration-test");
    topology.addOther("source", "producerAclsCreation");
    topology.addProject(project);

    return topology;
  }

  private Topology topologyWithQuotas() {
    List<Producer> producers = new ArrayList<>();
    Producer producer = new Producer("User:user1");
    producers.add(producer);
    Producer producer2 = new Producer("User:user2");
    producers.add(producer2);

    List<Consumer> consumers = new ArrayList<>();
    Consumer consumer = new Consumer("User:user1");
    consumers.add(consumer);
    Consumer consumer2 = new Consumer("User:user2");
    consumers.add(consumer2);

    Project project = new ProjectImpl("project");
    project.setProducers(producers);
    project.setConsumers(consumers);

    Topic topicA = new Topic("topicA");
    project.addTopic(topicA);

    Platform platform = new Platform();
    Kafka kafka = new Kafka();
    Quota quota = new Quota();
    quota.setPrincipal("User:user1");
    quota.setConsumer_byte_rate(Optional.of(1024d));
    quota.setProducer_byte_rate(Optional.of(1024d));
    quota.setRequest_percentage(Optional.of(80d));
    kafka.setQuotas(Optional.of(List.of(quota)));
    platform.setKafka(kafka);

    Topology topology = new TopologyImpl();
    topology.setContext("integration-test");
    topology.addOther("source", "producerAclsCreation");
    topology.addProject(project);
    topology.setPlatform(platform);

    return topology;
  }

  private Topology topologyWithoutQuotas() {
    List<Producer> producers = new ArrayList<>();
    Producer producer = new Producer("User:user1");
    producers.add(producer);
    Producer producer2 = new Producer("User:user2");
    producers.add(producer2);

    List<Consumer> consumers = new ArrayList<>();
    Consumer consumer = new Consumer("User:user1");
    consumers.add(consumer);
    Consumer consumer2 = new Consumer("User:user2");
    consumers.add(consumer2);

    Project project = new ProjectImpl("project");
    project.setProducers(producers);
    project.setConsumers(consumers);

    Topic topicA = new Topic("topicA");
    project.addTopic(topicA);

    Topology topology = new TopologyImpl();
    topology.setContext("integration-test");
    topology.addOther("source", "producerAclsCreation");
    topology.addProject(project);

    return topology;
  }

  @Test
  public void planUpdateTesting() throws IOException {
    // Test quota creation plan update
    Topology topology = topologyWithQuotas();
    quotasManager.updatePlan(topology, plan);
    plan.run();

    List<Action> actions = plan.getActions();

    assertEquals(1, actions.size());
    Action action = actions.get(0);
    assertTrue(action instanceof CreateQuotasAction);

    plan.getActions().clear();
    try {
      /* with CP 8.0.0 there are timing issues between updating quotas and querying them. */
      Thread.sleep(500);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    // Test NOOP plan update
    quotasManager.updatePlan(topology, plan);
    plan.run();

    actions = plan.getActions();

    assertTrue(actions.isEmpty());

    plan.getActions().clear();

    // Test quota deletion plan update with deletion disabled
    assertFalse(config.isAllowDeleteQuotas());
    topology = topologyWithoutQuotas();
    quotasManager.updatePlan(topology, plan);
    plan.run();

    actions = plan.getActions();
    assertTrue(actions.isEmpty());

    // Test quota deletion plan update with deletion enabled
    // Enable quota deletion
    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);
    props.put(ALLOW_DELETE_BINDINGS, true);
    props.put(ALLOW_DELETE_QUOTAS, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CCLOUD_ENV_CONFIG, "");

    config = new Configuration(cliOps, props);
    quotasManager = new QuotasManager(topologyAdminClient, config);

    assertTrue(config.isAllowDeleteQuotas());
    topology = topologyWithoutQuotas();
    quotasManager.updatePlan(topology, plan);
    plan.run();

    actions = plan.getActions();
    assertEquals(1, actions.size());
    action = actions.get(0);
    assertTrue(action instanceof DeleteQuotasAction);
  }

  @Test
  public void quotaForUserCreation() throws ExecutionException, InterruptedException, IOException {

    Topology topology = woldMSpecPattern();
    accessControlManager.updatePlan(topology, plan);
    plan.run();

    List<Quota> quotas = new ArrayList<>();
    Quota quota = new Quota("user1", Optional.empty(), Optional.of(20.0));
    quotas.add(quota);

    topologyAdminClient.assignQuotasPrincipal(quotas);
    // Verify Quotas
    verifyQuotasOnlyUser(quotas);
  }

  @Test
  public void quotaForUserRemove() throws ExecutionException, InterruptedException, IOException {

    Topology topology = woldMSpecPattern();
    accessControlManager.updatePlan(topology, plan);
    plan.run();

    List<Quota> quotas = new ArrayList<>();
    Quota quota = new Quota("user3", Optional.empty(), Optional.of(20.0));
    quotas.add(quota);

    topologyAdminClient.assignQuotasPrincipal(quotas);
    // Verify Quotas
    assertTrue(verifyQuotasOnlyUser(quotas).stream().allMatch(f -> f.equals(true)));

    // Remove quota
    topologyAdminClient.removeQuotasPrincipal(List.of(new User("user3")));
    assertTrue(verifyQuotasOnlyUser(quotas).stream().allMatch(f -> f.equals(false)));
  }

  @Test
  public void quotaForUserChangeValues()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = woldMSpecPattern();
    accessControlManager.updatePlan(topology, plan);
    plan.run();

    List<Quota> quotas = new ArrayList<>();
    Quota quota = new Quota("user1", Optional.empty(), Optional.of(20.0));
    quotas.add(quota);
    topologyAdminClient.assignQuotasPrincipal(quotas);
    // Verify Quotas
    assertTrue(verifyQuotasOnlyUser(quotas).stream().allMatch(f -> f.equals(true)));

    // change value
    Quota quotaUpdate = new Quota("user1", Optional.of(150.0), Optional.of(250.0));
    quotas.clear();
    quotas.add(quotaUpdate);
    topologyAdminClient.assignQuotasPrincipal(quotas);
    assertTrue(verifyQuotasOnlyUser(quotas).stream().allMatch(f -> f.equals(true)));
  }

  @Test
  public void quotaOnlyRemoveOneUser()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = woldMSpecPattern();
    accessControlManager.updatePlan(topology, plan);
    plan.run();

    List<Quota> quotas = new ArrayList<>();
    Quota quota = new Quota("user1", Optional.empty(), Optional.of(20.0));
    quotas.add(quota);
    Quota quota2 = new Quota("user2", Optional.of(300.0), Optional.of(100.0), Optional.of(50.0));
    quotas.add(quota2);
    topologyAdminClient.assignQuotasPrincipal(quotas);
    // Verify Quotas
    assertTrue(verifyQuotasOnlyUser(quotas).stream().allMatch(f -> f.equals(true)));

    topologyAdminClient.removeQuotasPrincipal(List.of(new User("user2")));
    assertTrue(verifyQuotasOnlyUser(List.of(quota)).stream().allMatch(f -> f.equals(true)));

    assertTrue(verifyQuotasOnlyUser(List.of(quota2)).stream().allMatch(f -> f.equals(false)));
  }

  private List<Boolean> verifyQuotasOnlyUser(List<Quota> quotas)
      throws ExecutionException, InterruptedException {
    Map<ClientQuotaEntity, Map<String, Double>> cqsresult =
        kafkaAdminClient.describeClientQuotas(ClientQuotaFilter.all()).entities().get();
    return quotas.stream()
        .map(
            q -> {
              ClientQuotaEntity cqe =
                  new ClientQuotaEntity(
                      Collections.singletonMap(ClientQuotaEntity.USER, q.getPrincipal()));
              if (cqsresult.containsKey(cqe)) {
                verifyQuotaAssigment(q, cqsresult.get(cqe));
                return true;
              } else {
                return false;
              }
            })
        .collect(Collectors.toList());
  }

  private void verifyQuotaAssigment(Quota q, Map<String, Double> map) {
    if (q.getProducer_byte_rate().isPresent()) {
      assertTrue(map.get("producer_byte_rate").equals(q.getProducer_byte_rate().get()));
    }
    if (q.getConsumer_byte_rate().isPresent()) {
      assertTrue(map.get("consumer_byte_rate").equals(q.getConsumer_byte_rate().get()));
    }
    if (q.getRequest_percentage().isPresent()) {
      assertTrue(map.get("request_percentage").equals(q.getRequest_percentage().get()));
    }
  }
}
