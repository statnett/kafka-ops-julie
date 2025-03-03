package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.CLIENT_CONFIG_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.DRY_RUN_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.QUIET_OPTION;

import com.purbon.kafka.topology.JulieOps;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextEmbeddedKafka;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JulieOpsIT {

  private static SaslPlaintextEmbeddedKafka kafka;

  @BeforeClass
  public static void setup() {
    kafka = new SaslPlaintextEmbeddedKafka();
    kafka.start();
  }

  @AfterClass
  public static void teardown() {
    kafka.stop();
  }

  @Test(expected = IOException.class)
  public void testSetupKafkaTopologyBuilderWithWrongCredentialsHC() throws Exception {

    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/wrong-client-config.properties");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, kafka.getBootstrapServers());
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
    config.put(BROKERS_OPTION, kafka.getBootstrapServers());
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "true");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    JulieOps.build(fileOrDirPath, config);
  }
}
