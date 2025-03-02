package com.purbon.kafka.topology.integration;

public class MDSBaseTest {

  public void beforeEach() {}

  protected String getKafkaClusterID() {
    /* NOTE: On 2025-02-25 the lookup in ZooKeeper was replaced by this string constant
     * in order to get rid of the ZooKeeper dependency.
     * The tests will probably fail, but since these MDS tests are excluded from the
     * pipeline, they will only fail on the original maintainer's setup.
     * Given access to a modern Admin client, the cluster id may be obtained like this:
     *   String clusterId = admin.describeCluster().clusterId().get();
     */
    return "kafka-cluster-id";
  }

  protected String getSchemaRegistryClusterID() {
    return "schema-registry";
  }

  protected String getKafkaConnectClusterID() {
    return "connect-cluster";
  }

  protected String getKSqlClusterID() {
    return "ksqldb";
  }
}
