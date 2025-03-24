package com.purbon.kafka.topology.backend.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.utils.JSON;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaBackendConsumer {

  private String instanceId;
  private Configuration config;
  private KafkaConsumer<String, byte[]> consumer;
  private ByteArrayChunker chunker;

  public KafkaBackendConsumer(Configuration config, ByteArrayChunker chunker) {
    this.config = config;
    this.chunker = chunker;
    this.instanceId = config.getJulieInstanceId();
  }

  public void configure() {
    Properties consumerProperties = config.asProperties();
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(GROUP_ID_CONFIG, config.getKafkaBackendConsumerGroupId());
    consumer = new KafkaConsumer<>(consumerProperties);
  }

  public void stop() {
    consumer.close();
  }

  public BackendState load() {
    Map<String, byte[]> map = new TreeMap<>(Comparator.comparing((String x) -> x));
    TopicPartition topicPartition = new TopicPartition(config.getKafkaBackendStateTopic(), 0);
    Collection<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);
    consumer.assign(topicPartitions);
    consumer.seekToEnd(topicPartitions);
    long lastOffset = consumer.position(topicPartition);
    consumer.seekToBeginning(topicPartitions);
    for (; ; ) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(5000));
      for (ConsumerRecord<String, byte[]> record : records) {
        if (record.key().startsWith(instanceId + "-")) {
          map.put(record.key(), record.value());
        }
      }
      if (consumer.position(topicPartition) >= lastOffset) {
        break;
      }
    }
    consumer.commitSync();
    if (map.isEmpty()) {
      return new BackendState();
    }
    byte[] bytes = chunker.dechunk(new ArrayList<>(map.values()));
    final String json = new String(bytes, StandardCharsets.UTF_8);
    try {
      return (BackendState) JSON.toObject(json, BackendState.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
