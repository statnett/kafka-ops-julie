package com.purbon.kafka.topology.integration.containerutils;

import java.util.HashMap;
import java.util.Map;
import no.shhsoft.k3aembedded.K3aEmbedded;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;

public final class SaslPlaintextEmbeddedKafka {

  private static final String JAAS_ADMIN_USER_LINE =
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka\" user_kafka=\"kafka\";";
  private K3aEmbedded kafka;

  public void start() {
    final Map<String, Object> map = new HashMap<>();
    map.put("listener.name.sasl_plaintext.sasl.enabled.mechanisms", "PLAIN");
    map.put("listener.name.sasl_plaintext.plain.sasl.jaas.config", JAAS_ADMIN_USER_LINE);
    map.put("listener.name.broker.plain.sasl.jaas.config", JAAS_ADMIN_USER_LINE);
    map.put("listener.name.controller.plain.sasl.jaas.config", JAAS_ADMIN_USER_LINE);
    map.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
    map.put("sasl.mechanism.controller.protocol", "PLAIN");
    map.put("sasl.enabled.mechanisms", "PLAIN");
    map.put("super.users", "User:kafka");
    map.put(
        "listener.security.protocol.map",
        "BROKER:SASL_PLAINTEXT, CONTROLLER:SASL_PLAINTEXT, SASL_PLAINTEXT:SASL_PLAINTEXT");
    map.put("authorizer.class.name", StandardAuthorizer.class.getName());
    kafka =
        new K3aEmbedded.Builder()
            .kraftMode(true)
            .additionalPorts(1)
            .additionalConfiguration(map)
            .additionalListenerWithPortIndex("SASL_PLAINTEXT", "SASL_PLAINTEXT", 0)
            .build();
    kafka.start();
  }

  public void stop() {
    kafka.stop();
  }

  public String getBootstrapServers() {
    return kafka.getBootstrapServers();
  }
}
