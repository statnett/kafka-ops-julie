package com.purbon.kafka.topology.model.users.platform;

import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.Quota;
import java.util.List;
import java.util.Optional;

public class Kafka {

  private Optional<List<User>> instances;
  private Optional<List<Quota>> quotas;

  public Kafka() {
    instances = Optional.empty();
    quotas = Optional.empty();
  }

  public Optional<List<User>> getInstances() {
    return instances;
  }

  public void setInstances(Optional<List<User>> instances) {
    this.instances = instances;
  }

  public Optional<List<Quota>> getQuotas() {
    return quotas;
  }

  public void setQuotas(Optional<List<Quota>> quotas) {
    this.quotas = quotas;
  }
}
