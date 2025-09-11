package com.purbon.kafka.topology.integration.containerutils;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class KsqlContainer extends GenericContainer<KsqlContainer> {

  private static final DockerImageName DEFAULT_IMAGE =
      DockerImageName.parse("confluentinc/ksqldb-server").withTag("0.26.0");
  /* ksqldb-server image only available for amd64, so running tests on Apple Silicon will take forever.
   * hence set a long timeout to be able to run the tests. */
  private static final Duration WAIT_DURATION = Duration.ofMinutes(20);

  public static final int KSQL_PORT = 8088;

  public KsqlContainer(AlternativeKafkaContainer kafka) {
    this(DEFAULT_IMAGE, kafka);
  }

  public KsqlContainer(final DockerImageName dockerImageName, AlternativeKafkaContainer kafka) {
    super(dockerImageName);
    withStartupTimeout(WAIT_DURATION);
    String kafkaHost = kafka.getNetworkAliases().get(1);
    withExposedPorts(KSQL_PORT);
    waitingFor(
        Wait.forLogMessage(".+ INFO Server up and running .+", 1)
            .withStartupTimeout(WAIT_DURATION));
    // withEnv("KSQL_KSQL_SERVICE_ID", "confluent_ksql_streams_01");
    withEnv("KSQL_SECURITY_PROTOCOL", "SASL_PLAINTEXT");
    withEnv("KSQL_BOOTSTRAP_SERVERS", "SASL_PLAINTEXT://" + kafkaHost + ":" + 9091);
    withEnv("KSQL_SASL_JAAS_CONFIG", saslConfig());
    withEnv("KSQL_SASL_MECHANISM", "PLAIN");
    withEnv("KSQL_LISTENERS", "http://0.0.0.0:" + KSQL_PORT);
    withEnv("KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE", "true");
    withEnv("KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE", "true");
    withNetwork(kafka.getNetwork());
  }

  private String saslConfig() {
    StringBuilder sb = new StringBuilder();
    sb.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_USERNAME);
    sb.append("\" password=\"");
    sb.append(ContainerTestUtils.DEFAULT_SUPER_PASSWORD);
    sb.append("\";");
    return sb.toString();
  }

  public String getUrl() {
    return "http://" + getHost() + ":" + getMappedPort(KSQL_PORT);
  }

  public Integer getPort() {
    return getMappedPort(KSQL_PORT);
  }
}
