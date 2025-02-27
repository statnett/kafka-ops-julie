package com.purbon.kafka.topology.backend;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.kafka.ByteArrayChunker;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendConsumer;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendProducer;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendTopicCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaBackend implements Backend {

  private static final Logger LOGGER = LogManager.getLogger(KafkaBackend.class);

  private ByteArrayChunker chunker;
  private KafkaBackendTopicCreator topicCreator;
  private KafkaBackendConsumer consumer;
  private KafkaBackendProducer producer;
  private boolean isDryRun = false;
  private boolean topicWasJustCreated = false;

  public KafkaBackend() {}

  @Override
  public void configure(Configuration config) {
    isDryRun = config.isDryRun();
    chunker = new ByteArrayChunker(config.getKafkaBackendChunkSize());

    topicCreator = new KafkaBackendTopicCreator(config);
    topicCreator.configure();
    topicWasJustCreated = topicCreator.createStateTopicUnlessPresent();

    consumer = new KafkaBackendConsumer(config, chunker);
    consumer.configure();

    producer = new KafkaBackendProducer(config, chunker);
    producer.configure();
  }

  @Override
  public void save(BackendState state) {
    producer.save(state);
  }

  @Override
  public BackendState load() {
    if (isDryRun && topicWasJustCreated) {
      return new BackendState();
    }
    return consumer.load();
  }

  @Override
  public void close() {
    consumer.stop();
    producer.stop();
  }
}
