package com.purbon.kafka.topology.api.adminclient;

import com.purbon.kafka.topology.actions.topics.TopicConfigUpdatePlan;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.Quota;
import com.purbon.kafka.topology.quotas.QuotasClientBindingsBuilder;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TopologyBuilderAdminClient {

  private static final Logger LOGGER = LogManager.getLogger(TopologyBuilderAdminClient.class);

  private final AdminClient adminClient;

  public TopologyBuilderAdminClient(AdminClient adminClient) {
    this.adminClient = adminClient;
  }

  public Set<String> listTopics(ListTopicsOptions options) throws IOException {
    Set<String> listOfTopics;
    try {
      listOfTopics = adminClient.listTopics(options).names().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
    return listOfTopics;
  }

  public void healthCheck() throws IOException {

    try {
      adminClient.describeCluster().nodes().get();
    } catch (Exception ex) {
      throw new IOException("Problem during the health-check operation", ex);
    }
  }

  public Set<String> listTopics() throws IOException {
    return listTopics(new ListTopicsOptions());
  }

  public Set<String> listApplicationTopics() throws IOException {
    ListTopicsOptions options = new ListTopicsOptions();
    options.listInternal(false);
    return listTopics(options);
  }

  public void updateTopicConfig(TopicConfigUpdatePlan configUpdatePlan) {
    Set<AlterConfigOp> configChanges = new HashSet<>();

    configUpdatePlan
        .getNewConfigValues()
        .forEach(
            (configKey, configValue) ->
                configChanges.add(
                    new AlterConfigOp(new ConfigEntry(configKey, configValue), OpType.SET)));

    configUpdatePlan
        .getUpdatedConfigValues()
        .forEach(
            (configKey, configValue) ->
                configChanges.add(
                    new AlterConfigOp(new ConfigEntry(configKey, configValue), OpType.SET)));

    configUpdatePlan
        .getDeletedConfigValues()
        .forEach(
            (configKey, configValue) ->
                configChanges.add(
                    new AlterConfigOp(new ConfigEntry(configKey, configValue), OpType.DELETE)));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
    configs.put(new ConfigResource(Type.TOPIC, configUpdatePlan.getFullTopicName()), configChanges);

    try {
      adminClient.incrementalAlterConfigs(configs).all().get();
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.error("Failed to update configs for topic " + configUpdatePlan.getFullTopicName(), ex);
      throw new RuntimeException(ex);
    }
  }

  public int getPartitionCount(String topic) throws IOException {
    try {
      Map<String, TopicDescription> results =
          adminClient.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
      return results.get(topic).partitions().size();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
  }

  public void updatePartitionCount(Topic topic, String topicName) throws IOException {
    Map<String, NewPartitions> map = new HashMap<>();
    map.put(topicName, NewPartitions.increaseTo(topic.partitionsCount()));
    try {
      adminClient.createPartitions(map).all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
  }

  public void clearAcls(TopologyAclBinding aclBinding) throws IOException {
    Collection<AclBindingFilter> filters = new ArrayList<>();

    LOGGER.debug("clearAcl = " + aclBinding);
    ResourcePatternFilter resourceFilter =
        new ResourcePatternFilter(
            ResourceType.valueOf(aclBinding.getResourceType()),
            aclBinding.getResourceName(),
            PatternType.valueOf(aclBinding.getPattern()));

    AccessControlEntryFilter accessControlEntryFilter =
        new AccessControlEntryFilter(
            aclBinding.getPrincipal(),
            aclBinding.getHost(),
            AclOperation.valueOf(aclBinding.getOperation()),
            AclPermissionType.ANY);

    AclBindingFilter filter = new AclBindingFilter(resourceFilter, accessControlEntryFilter);
    filters.add(filter);
    clearAcls(filters);
  }

  private void clearAcls(Collection<AclBindingFilter> filters) throws IOException {
    try {
      adminClient.deleteAcls(filters).all().get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
  }

  public Config getActualTopicConfig(String topic) {
    ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    Collection<ConfigResource> resources = Collections.singletonList(resource);

    Map<ConfigResource, Config> configs = null;
    try {
      configs = adminClient.describeConfigs(resources).all().get();
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.error(ex);
      throw new RuntimeException(ex);
    }

    return configs.get(resource);
  }

  public void createTopic(Topic topic, String fullTopicName) throws IOException {
    NewTopic newTopic =
        new NewTopic(fullTopicName, topic.getPartitionCount(), topic.replicationFactor())
            .configs(topic.getRawConfig());
    try {
      createAllTopics(Collections.singleton(newTopic));
    } catch (ExecutionException | InterruptedException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOGGER.info(e.getMessage());
        return;
      }
      LOGGER.error(e);
      throw new IOException(e);
    }
  }

  public void createTopic(String topicName) throws IOException {
    Topic topic = new Topic();
    createTopic(topic, topicName);
  }

  private void createAllTopics(Collection<NewTopic> newTopics)
      throws ExecutionException, InterruptedException {
    adminClient.createTopics(newTopics).all().get();
  }

  public void deleteTopics(Collection<String> topics) throws IOException {
    try {
      adminClient.deleteTopics(topics).all().get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
  }

  public Map<String, Collection<AclBinding>> fetchAclsList() {
    Map<String, Collection<AclBinding>> acls = new HashMap<>();

    try {
      Collection<AclBinding> list = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
      list.forEach(
          aclBinding -> {
            String name = aclBinding.pattern().name();
            acls.computeIfAbsent(name, k -> new ArrayList<>());
            Collection<AclBinding> updatedList = acls.get(name);
            updatedList.add(aclBinding);
            acls.put(name, updatedList);
          });
    } catch (Exception e) {
      return new HashMap<>();
    }
    return acls;
  }

  public void createAcls(Collection<AclBinding> acls) {
    try {
      String aclsDump = acls.stream().map(AclBinding::toString).collect(Collectors.joining(", "));
      LOGGER.debug("createAcls: " + aclsDump);
      adminClient.createAcls(acls).all().get();
    } catch (InvalidConfigurationException ex) {
      LOGGER.error(ex);
      throw ex;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error(e);
    }
  }

  public void assignQuotasPrincipal(Collection<Quota> quotas) {
    List<ClientQuotaAlteration> lstQuotasAlteration =
        quotas.stream()
            .map(f -> new QuotasClientBindingsBuilder(f).build())
            .collect(Collectors.toList());
    try {
      this.adminClient.alterClientQuotas(lstQuotasAlteration).all().get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  public void removeQuotasPrincipal(Collection<User> users) {
    List<ClientQuotaAlteration> lstQuotasRemove =
        users.stream()
            .map(
                f ->
                    new QuotasClientBindingsBuilder(
                            new Quota(
                                f.getPrincipal(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .build())
            .collect(Collectors.toList());
    try {
      this.adminClient.alterClientQuotas(lstQuotasRemove).all().get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  public Map<ClientQuotaEntity, Map<String, Double>> describeClientQuotas()
      throws ExecutionException, InterruptedException {
    return this.adminClient.describeClientQuotas(ClientQuotaFilter.all()).entities().get();
  }

  public void close() {
    adminClient.close();
  }
}
