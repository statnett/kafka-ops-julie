package com.purbon.kafka.topology.integration.backend;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.DRY_RUN_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.backend.KafkaBackend;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.util.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaBackendIT {

  Configuration config;
  Properties props;
  Map<String, String> cliOps;

  private static SaslPlaintextKafkaContainer container;

  @Before
  public void before() {

    container = ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));
    container.start();
    ContainerTestUtils.resetAcls(container);

    props = new Properties();
    props.put(JULIE_INSTANCE_ID, "1234");
    props.put(JULIE_KAFKA_STATE_CHUNK_SIZE, 16 /* small to enforce chunking */);

    Map<String, Object> saslConfig =
        ContainerTestUtils.getSaslConfig(
            container.getBootstrapServers(),
            SaslPlaintextKafkaContainer.JULIE_USERNAME,
            SaslPlaintextKafkaContainer.JULIE_PASSWORD);
    saslConfig.forEach((k, v) -> props.setProperty(k, String.valueOf(v)));

    cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, container.getBootstrapServers());

    config = new Configuration(cliOps, props);
  }

  @After
  public void after() {
    container.stop();
  }

  @Test
  public void testExpectedFlow() throws InterruptedException {

    TopologyAclBinding binding1 =
        TopologyAclBinding.build(
            ResourceType.TOPIC.name(), "foo", "*", "Write", "User:foo", "LITERAL");
    TopologyAclBinding binding2 =
        TopologyAclBinding.build(
            ResourceType.TOPIC.name(), "bar", "*", "Read", "User:bar", "LITERAL");
    Collection<TopologyAclBinding> bindings1 = Collections.singleton(binding1);
    Collection<TopologyAclBinding> bindings2 = Arrays.asList(binding1, binding2);
    Collection<TopologyAclBinding> bindings3 = Collections.singleton(binding2);

    saveBindings(bindings1);
    loadAndVerifyBindings(bindings1);

    saveBindings(bindings2);
    loadAndVerifyBindings(bindings2);

    saveBindings(bindings3);
    loadAndVerifyBindings(bindings3);
  }

  @Test
  public void shouldFailOnNonCompactingStateTopic() {
    String wrongTopic = "__state_with_wrong_cleanup_policy";
    AdminClient admin =
        ContainerTestUtils.getSaslSuperUserAdminClient(container.getBootstrapServers());
    Map<String, String> topicConfig = new HashMap<>();
    topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    NewTopic configTopic = new NewTopic(wrongTopic, 1, (short) 1);
    try {
      admin.createTopics(Collections.singleton(configTopic)).all().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Properties overriddenProps = new Properties();
    props.forEach((k, v) -> overriddenProps.setProperty((String) k, String.valueOf(v)));
    overriddenProps.setProperty(JULIE_KAFKA_STATE_TOPIC, wrongTopic);
    KafkaBackend backend = new KafkaBackend();
    try {
      backend.configure(new Configuration(cliOps, overriddenProps));
      Assert.fail("Expected expection since topis is not compacting");
    } catch (Exception e) {
      Assert.assertTrue(
          "Unexpected exception", e.getMessage().contains(TopicConfig.CLEANUP_POLICY_COMPACT));
    }
  }

  @Test
  public void shouldWorkOnNonExistingStateTopicInDryRun() {
    String nonExistingTopic = "__non_existing";
    Properties overriddenProps = new Properties();
    props.forEach((k, v) -> overriddenProps.setProperty((String) k, String.valueOf(v)));
    overriddenProps.setProperty(JULIE_KAFKA_STATE_TOPIC, nonExistingTopic);
    Map<String, String> overriddenCliOps = new HashMap<>(cliOps);
    overriddenCliOps.put(DRY_RUN_OPTION, "true");
    KafkaBackend backend = new KafkaBackend();
    backend.configure(new Configuration(overriddenCliOps, overriddenProps));
    backend.save(new BackendState());
    backend.load();
  }

  private void saveBindings(Collection<TopologyAclBinding> bindings) {
    BackendState state = new BackendState();
    state.addBindings(bindings);
    KafkaBackend backend = new KafkaBackend();
    backend.configure(config);
    backend.save(state);
    backend.close();
  }

  private void loadAndVerifyBindings(Collection<TopologyAclBinding> expectedBindings) {
    KafkaBackend backend = new KafkaBackend();
    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, container.getBootstrapServers());
    Configuration config = new Configuration(cliOps, props);
    backend.configure(config);
    BackendState state = backend.load();
    assertThat(state.size()).isEqualTo(expectedBindings.size());
    for (TopologyAclBinding binding : expectedBindings) {
      assertThat(state.getBindings()).contains(binding);
    }
    backend.close();
  }
}
