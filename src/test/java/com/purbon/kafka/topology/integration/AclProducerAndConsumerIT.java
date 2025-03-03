package com.purbon.kafka.topology.integration;

import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextEmbeddedKafka;
import com.purbon.kafka.topology.integration.containerutils.TestConsumer;
import com.purbon.kafka.topology.integration.containerutils.TestProducer;
import java.util.Set;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public final class AclProducerAndConsumerIT {

  private static final String TOPIC = "producer-and-consumer-test-topic";
  private static final String OTHER_TOPIC = "other-" + TOPIC;
  private static final String UNKNOWN_USERNAME = "unknown-user";
  private static final String CONSUMER_GROUP = "producer-and-consumer-test-consumer-group";
  private static SaslPlaintextEmbeddedKafka kafka;

  @BeforeClass
  public static void beforeClass() {
    kafka = new SaslPlaintextEmbeddedKafka();
    kafka.start();
    ContainerTestUtils.resetAcls(kafka);
    ContainerTestUtils.populateAcls(
        kafka, "/acl-producer-and-consumer-it.yaml", "/integration-tests.properties");
  }

  @AfterClass
  public static void afterClass() {
    kafka.stop();
  }

  @Test(expected = SaslAuthenticationException.class)
  public void shouldNotProduceWhenUnknownUser() {
    try (final TestProducer producer = TestProducer.create(kafka, UNKNOWN_USERNAME)) {
      producer.produce(TOPIC, "foo");
    }
  }

  @Test(expected = SaslAuthenticationException.class)
  public void shouldNotConsumeWhenUnknownUser() {
    try (final TestConsumer consumer =
        TestConsumer.create(kafka, UNKNOWN_USERNAME, CONSUMER_GROUP)) {
      consumer.consumeForAWhile(TOPIC, null);
    }
  }

  @Test(expected = TopicAuthorizationException.class)
  public void shouldNotProduceWithoutPermission() {
    try (final TestProducer producer =
        TestProducer.create(kafka, ContainerTestUtils.NO_ACCESS_USERNAME)) {
      producer.produce(TOPIC, "foo");
    }
  }

  @Test(expected = TopicAuthorizationException.class)
  public void shouldNotConsumeWithoutPermission() {
    try (final TestConsumer consumer =
        TestConsumer.create(kafka, ContainerTestUtils.NO_ACCESS_USERNAME, CONSUMER_GROUP)) {
      consumer.consumeForAWhile(TOPIC, null);
    }
  }

  @Test(expected = TopicAuthorizationException.class)
  public void shouldNotProduceWithoutPermissionEvenIfPermittedElsewhere() {
    try (final TestProducer producer =
        TestProducer.create(kafka, ContainerTestUtils.OTHER_PRODUCER_USERNAME)) {
      producer.produce(TOPIC, "foo");
    }
  }

  @Test(expected = TopicAuthorizationException.class)
  public void shouldNotConsumeWithoutPermissionEvenIfPermittedElsewhere() {
    try (final TestConsumer consumer =
        TestConsumer.create(kafka, ContainerTestUtils.OTHER_CONSUMER_USERNAME, CONSUMER_GROUP)) {
      consumer.consumeForAWhile(TOPIC, null);
    }
  }

  @Test(expected = TopicAuthorizationException.class)
  public void shouldNotProduceWhenConsumer() {
    try (final TestProducer producer =
        TestProducer.create(kafka, ContainerTestUtils.CONSUMER_USERNAME)) {
      producer.produce(TOPIC, "foo");
    }
  }

  @Test(expected = GroupAuthorizationException.class)
  public void shouldNotConsumeWhenProducer() {
    try (final TestConsumer consumer =
        TestConsumer.create(kafka, ContainerTestUtils.PRODUCER_USERNAME, CONSUMER_GROUP)) {
      consumer.consumeForAWhile(TOPIC, null);
    }
  }

  @Test
  public void shouldProduceAndConsume() {
    produceAndConsume(
        TOPIC, ContainerTestUtils.PRODUCER_USERNAME, ContainerTestUtils.CONSUMER_USERNAME);
  }

  @Test
  public void shouldProduceAndConsumeElsewhere() {
    /* Just to test that everything was spelled correctly in source and config for the above tests. */
    produceAndConsume(
        OTHER_TOPIC,
        ContainerTestUtils.OTHER_PRODUCER_USERNAME,
        ContainerTestUtils.OTHER_CONSUMER_USERNAME);
  }

  private void produceAndConsume(
      final String topicName, final String producerUsername, final String consumerUsername) {
    try (final TestProducer producer = TestProducer.create(kafka, producerUsername);
        final TestConsumer consumer =
            TestConsumer.create(kafka, consumerUsername, CONSUMER_GROUP)) {
      final Set<String> values = producer.produceSomeStrings(topicName);
      consumer.consumeForAWhile(
          topicName,
          (key, value) -> {
            values.remove(value);
            return !values.isEmpty();
          });
      if (!values.isEmpty()) {
        Assert.fail("Unable to consume all messages.");
      }
    }
  }
}
