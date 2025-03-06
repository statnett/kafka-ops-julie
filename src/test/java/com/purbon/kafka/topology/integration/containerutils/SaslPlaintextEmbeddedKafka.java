package com.purbon.kafka.topology.integration.containerutils;

import java.util.HashMap;
import java.util.Map;
import no.shhsoft.k3aembedded.K3aEmbedded;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.testcontainers.Testcontainers;

public final class SaslPlaintextEmbeddedKafka {

  private static final String[] EXTRA_USERS =
      new String[] {
        ContainerTestUtils.NO_ACCESS_USERNAME,
        ContainerTestUtils.PRODUCER_USERNAME,
        ContainerTestUtils.CONSUMER_USERNAME,
        ContainerTestUtils.OTHER_PRODUCER_USERNAME,
        ContainerTestUtils.OTHER_CONSUMER_USERNAME,
        ContainerTestUtils.STREAMS_USERNAME,
      };
  private K3aEmbedded kafka;

  public void start() {
    final String jaasLoginLine = createJaasLoginLine();
    final Map<String, Object> map = new HashMap<>();
    map.put("listener.name.broker.plain.sasl.jaas.config", jaasLoginLine);
    map.put("listener.name.controller.plain.sasl.jaas.config", jaasLoginLine);
    map.put("listener.name.sasl_plaintext.sasl.enabled.mechanisms", "PLAIN");
    map.put("listener.name.sasl_plaintext.plain.sasl.jaas.config", jaasLoginLine);
    map.put("listener.name.testcontainers.sasl.enabled.mechanisms", "PLAIN");
    map.put("listener.name.testcontainers.plain.sasl.jaas.config", jaasLoginLine);
    map.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
    map.put("sasl.mechanism.controller.protocol", "PLAIN");
    map.put("sasl.enabled.mechanisms", "PLAIN");
    map.put("super.users", "User:kafka");
    map.put(
        "listener.security.protocol.map",
        "BROKER:SASL_PLAINTEXT, CONTROLLER:SASL_PLAINTEXT, SASL_PLAINTEXT:SASL_PLAINTEXT, TESTCONTAINERS:SASL_PLAINTEXT");
    map.put("authorizer.class.name", StandardAuthorizer.class.getName());
    kafka =
        new K3aEmbedded.Builder()
            .kraftMode(true)
            .additionalPorts(2)
            .additionalConfiguration(map)
            .additionalListenerWithPortIndex("SASL_PLAINTEXT", "SASL_PLAINTEXT", 0)
            .additionalListenerWithPortIndex("TESTCONTAINERS", "SASL_PLAINTEXT", 1)
            .additionalConfigurationProvider(
                () -> {
                  return Map.of(
                      "advertised.listeners",
                      "BROKER://:"
                          + kafka.getBrokerPort()
                          + ","
                          + "CONTROLLER://:"
                          + kafka.getControllerPort()
                          + ","
                          + "SASL_PLAINTEXT://:"
                          + kafka.getAdditionalPort(0)
                          + ","
                          + "TESTCONTAINERS://host.testcontainers.internal:"
                          + kafka.getAdditionalPort(1));
                })
            .build();
    kafka.start();
    Testcontainers.exposeHostPorts(kafka.getAdditionalPort(1));
  }

  public void stop() {
    kafka.stop();
  }

  public String getBootstrapServers() {
    return kafka.getBootstrapServersForAdditionalPort(0);
  }

  public String getBootstrapServersForTestContainers() {
    return "TESTCONTAINERS://host.testcontainers.internal:" + kafka.getAdditionalPort(1);
  }

  private String createJaasLoginLine() {
    /* Precondition: No usernames or passwords contain characters that need special handling for JAAS config. */
    final StringBuilder sb = new StringBuilder();
    sb.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_USERNAME);
    sb.append("\" password=\"");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_USERNAME);
    sb.append("\" user_");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_USERNAME);
    sb.append("=\"");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_USERNAME);
    sb.append("\" user_");
    sb.append(ContainerTestUtils.JULIE_USERNAME);
    sb.append("=\"");
    sb.append(ContainerTestUtils.JULIE_PASSWORD);
    sb.append("\"");
    for (final String userAndPassword : EXTRA_USERS) {
      assertValidUsernameAndPassword(userAndPassword);
      sb.append(" user_");
      sb.append(userAndPassword);
      sb.append("=\"");
      sb.append(userAndPassword);
      sb.append("\"");
    }
    sb.append(";");
    return sb.toString();
  }

  private static String assertValidUsernameAndPassword(final String s) {
    /* Enforcing, in order to not have to deal with escaping for the JAAS config. */
    for (final char c : s.toCharArray()) {
      if (!(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-')) {
        throw new RuntimeException(
            "Only letters, digits and hyphens allowed in usernames and passwords.");
      }
    }
    return s;
  }
}
