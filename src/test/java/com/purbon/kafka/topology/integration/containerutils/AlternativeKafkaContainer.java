package com.purbon.kafka.topology.integration.containerutils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.Uuid;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * An alternative KafkaContainer that is easier to extend than the one from the testcontainers
 * project version 1.15.0.
 */
public class AlternativeKafkaContainer extends GenericContainer<AlternativeKafkaContainer> {

  private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
  public static final String INTERNAL_LISTENER_NAME = "BROKER";
  public static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
  public static final int KAFKA_PORT = 9092;
  public static final int KAFKA_INTERNAL_PORT = 9093;
  public static final int KAFKA_CONTROLLER_PORT = 9090;
  /* Note difference between 0.0.0.0 and localhost: The former will be replaced by the container IP. */
  private static final String LISTENERS =
      "PLAINTEXT://0.0.0.0:"
          + KAFKA_PORT
          + ","
          + INTERNAL_LISTENER_NAME
          + "://127.0.0.1:"
          + KAFKA_INTERNAL_PORT
          + ","
          + CONTROLLER_LISTENER_NAME
          + "://127.0.0.1:"
          + KAFKA_CONTROLLER_PORT;
  private static final int PORT_NOT_ASSIGNED = -1;
  private int port = PORT_NOT_ASSIGNED;

  public AlternativeKafkaContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    withExposedPorts(KAFKA_PORT, KAFKA_CONTROLLER_PORT);
    withEnv("KAFKA_LISTENERS", LISTENERS);
    withEnv(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "PLAINTEXT:PLAINTEXT,"
            + INTERNAL_LISTENER_NAME
            + ":PLAINTEXT,"
            + CONTROLLER_LISTENER_NAME
            + ":PLAINTEXT");
    withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", INTERNAL_LISTENER_NAME);
    withEnv("KAFKA_NODE_ID", "1");
    withEnv("KAFKA_PROCESS_ROLES", "broker, controller");
    withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:" + KAFKA_CONTROLLER_PORT);
    withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", CONTROLLER_LISTENER_NAME);
    withEnv("CLUSTER_ID", Uuid.randomUuid().toString());
    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
  }

  public final String getBootstrapServers() {
    if (port == PORT_NOT_ASSIGNED) {
      throw new IllegalStateException("You should start Kafka container first");
    }
    return overrideBootstrapServers(String.format("%s:%s", getHost(), port));
  }

  /** Subclasses may override. */
  protected String overrideBootstrapServers(final String bootstrapServers) {
    return bootstrapServers;
  }

  @Override
  protected final void doStart() {
    withCommand(
        "sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
    beforeStart();
    super.doStart();
  }

  /** Subclasses may override. */
  protected void beforeStart() {}

  @Override
  protected final void containerIsStarting(
      final InspectContainerResponse containerInfo, final boolean reused) {
    super.containerIsStarting(containerInfo, reused);
    port = getMappedPort(KAFKA_PORT);
    if (reused) {
      return;
    }
    beforeStartupPreparations();
    createStartupScript();
  }

  /** Subclasses may override. */
  protected void beforeStartupPreparations() {}

  private void createStartupScript() {
    final String listeners = getEnvMap().get("KAFKA_LISTENERS");
    if (listeners == null) {
      throw new RuntimeException("Need environment variable KAFKA_LISTENERS");
    }
    final String advertisedListeners =
        overrideAdvertisedListeners(
            listeners
                .replaceAll(":" + KAFKA_PORT, ":" + getMappedPort(KAFKA_PORT))
                .replaceAll("OTHER://0\\.0\\.0\\.0", "OTHER://kafka")
                .replaceAll("0\\.0\\.0\\.0", getHost()));
    final String startupScript =
        overrideStartupScript(
            "#!/bin/bash\n"
                + "export KAFKA_ADVERTISED_LISTENERS='"
                + advertisedListeners
                + "'\n"
                + ". /etc/confluent/docker/bash-config\n"
                + "/etc/confluent/docker/configure\n"
                + "/etc/confluent/docker/ensure\n"
                + "/etc/confluent/docker/launch\n");
    copyFileToContainer(
        Transferable.of(startupScript.getBytes(StandardCharsets.UTF_8), 0755), STARTER_SCRIPT);
  }

  /** Subclasses may override. */
  protected String overrideAdvertisedListeners(final String advertisedListeners) {
    return advertisedListeners;
  }

  /** Subclasses may override. */
  protected String overrideStartupScript(final String startupScript) {
    return startupScript;
  }

  @Override
  protected void containerIsStarted(InspectContainerResponse containerInfo) {
    super.containerIsStarted(containerInfo);
    System.out.println(getLogs());
  }
}
