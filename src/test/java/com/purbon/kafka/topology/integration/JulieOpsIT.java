package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.*;

import com.purbon.kafka.topology.JulieOps;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JulieOpsIT {

  private static SaslPlaintextKafkaContainer container;

  @BeforeClass
  public static void setup() {
    container = new SaslPlaintextKafkaContainer();
    container.start();
  }

  @AfterClass
  public static void teardown() {
    container.stop();
  }

  @Test(expected = IOException.class)
  public void testSetupKafkaTopologyBuilderWithWrongCredentialsHC() throws Exception {

    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/wrong-client-config.properties");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, container.getBootstrapServers());
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "true");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    JulieOps.build(fileOrDirPath, config);
  }

  @Test
  public void testSetupKafkaTopologyBuilderWithGoodCredentialsHC() throws Exception {

    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, container.getBootstrapServers());
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "true");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    JulieOps.build(fileOrDirPath, config);
  }

  @Test
  public void testSetupKafkaTopologyBuilderWithDir() throws Exception {
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");
    String fileOrDirPath = TestUtils.getResourceFilename("/dir");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, container.getBootstrapServers());
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "false");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    var ops = JulieOps.build(fileOrDirPath, config);
    ops.run();

    AdminClient kafkaAdminClient =
        ContainerTestUtils.getSaslSuperUserAdminClient(container.getBootstrapServers());

    var topics = kafkaAdminClient.listTopics().names().get();

    assert topics.size() == 9;
    assert topics.contains("contextOrg.source.foo.bar.zet.zet.foo");
    assert topics.contains("contextOrg.source.foo.bar.zet.zet.bar.avro");
    assert topics.contains("contextOrg.source.foo.foo");
    assert topics.contains("contextOrg.source.foo.bar.zet.bear.bar.avro");
    assert topics.contains("contextOrg.source.bar.bar.avro");
    assert topics.contains("contextOrg.source.foo.bar.avro");
    assert topics.contains("contextOrg.source.external.aaa.foo");
    assert topics.contains("i-am-very-special");
    assert topics.contains("i-am-more-special");
  }
}
