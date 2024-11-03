package com.purbon.kafka.topology.backend;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendConsumer;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendProducer;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendTopicCreator;
import com.purbon.kafka.topology.backend.kafka.RecordReceivedCallback;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaBackend implements Backend, RecordReceivedCallback {

  private static final Logger LOGGER = LogManager.getLogger(KafkaBackend.class);

  private boolean isCompleted;

  private KafkaBackendTopicCreator topicCreator;
  private KafkaBackendConsumer consumer;
  private KafkaBackendProducer producer;

  private AtomicReference<BackendState> latest;
  private AtomicBoolean shouldWaitForLoad;
  private String instanceId;
  private Thread thread;

  public KafkaBackend() {
    isCompleted = false;
    shouldWaitForLoad = new AtomicBoolean(true);
  }

  private static class JulieKafkaConsumerThread implements Runnable {
    private KafkaBackend callback;
    private KafkaBackendConsumer consumer;

    public JulieKafkaConsumerThread(KafkaBackend callback, KafkaBackendConsumer consumer) {
      this.callback = callback;
      this.consumer = consumer;
    }

    public void run() {
      consumer.start();
      try {
        consumer.retrieve(callback);
      } catch (WakeupException ex) {
        LOGGER.trace(ex);
      }
    }
  }

  @SneakyThrows
  @Override
  public void configure(Configuration config) {
    instanceId = config.getJulieInstanceId();
    latest = new AtomicReference<>(new BackendState());
    shouldWaitForLoad.set(true);

    topicCreator = new KafkaBackendTopicCreator(config);
    topicCreator.configure();
    topicCreator.createConfigTopicUnlessPresent();

    consumer = new KafkaBackendConsumer(config);
    consumer.configure();

    producer = new KafkaBackendProducer(config);
    producer.configure();

    thread = new Thread(new JulieKafkaConsumerThread(this, consumer), "kafkaJulieConsumer");
    thread.start();
    waitForCompletion();
  }

  public synchronized void waitForCompletion() throws InterruptedException {
    while (!isCompleted) {
      wait(30000);
    }
  }

  public synchronized void complete() {
    isCompleted = true;
    notify();
  }

  @Override
  public void save(BackendState state) {
    producer.save(state);
  }

  @SneakyThrows
  @Override
  public BackendState load() {
    while (shouldWaitForLoad.get()) {
      continue;
    }
    return latest == null ? new BackendState() : latest.get();
  }

  public void initialLoadFinish() {
    shouldWaitForLoad.set(false);
  }

  @Override
  public void close() {
    consumer.stop();
    producer.stop();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
    latest = null;
    thread = null;
  }

  @Override
  public void apply(ConsumerRecord<String, BackendState> record) {
    if (instanceId.equals(record.key()) && latest != null) {
      latest.set(record.value());
    }
  }
}
