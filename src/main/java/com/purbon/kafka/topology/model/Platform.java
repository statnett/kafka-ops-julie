package com.purbon.kafka.topology.model;

import com.purbon.kafka.topology.model.users.platform.Kafka;
import com.purbon.kafka.topology.model.users.platform.KafkaConnect;
import com.purbon.kafka.topology.model.users.platform.KsqlServer;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistry;

public class Platform {

  private Kafka kafka;
  private KafkaConnect kafkaConnect;
  private SchemaRegistry schemaRegistry;
  private KsqlServer ksqlServer;

  public Platform() {
    this.kafka = new Kafka();
    this.kafkaConnect = new KafkaConnect();
    this.schemaRegistry = new SchemaRegistry();
    this.ksqlServer = new KsqlServer();
  }

  public Kafka getKafka() {
    return kafka;
  }

  public void setKafka(Kafka kafka) {
    this.kafka = kafka;
  }

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  public void setSchemaRegistry(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public KafkaConnect getKafkaConnect() {
    return kafkaConnect;
  }

  public void setKafkaConnect(KafkaConnect kafkaConnect) {
    this.kafkaConnect = kafkaConnect;
  }

  public KsqlServer getKsqlServer() {
    return ksqlServer;
  }

  public void setKsqlServer(KsqlServer ksqlServer) {
    this.ksqlServer = ksqlServer;
  }
}
