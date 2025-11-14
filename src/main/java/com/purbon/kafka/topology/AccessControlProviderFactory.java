package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.Constants.*;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.roles.CCloudAclsProvider;
import com.purbon.kafka.topology.roles.HybridCCloudAclsProvider;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.roles.acls.AclsBindingsBuilder;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class AccessControlProviderFactory {

  private final Configuration config;
  private final TopologyBuilderAdminClient builderAdminClient;

  public AccessControlProviderFactory(
      Configuration config, TopologyBuilderAdminClient builderAdminClient) {
    this.config = config;
    this.builderAdminClient = builderAdminClient;
  }

  public AccessControlProvider get() throws IOException {

    String accessControlClassName = config.getAccessControlClassName();

    try {
      Class<?> clazz = Class.forName(accessControlClassName);
      switch (accessControlClassName) {
        case ACCESS_CONTROL_DEFAULT_CLASS:
          Constructor<?> aclsProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class);
          return (SimpleAclsProvider) aclsProviderConstructor.newInstance(builderAdminClient);
        case CONFLUENT_CLOUD_CONTROL_CLASS:
          Constructor<?> ccloudProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class, Configuration.class);
          return (CCloudAclsProvider)
              ccloudProviderConstructor.newInstance(builderAdminClient, config);
        case CONFLUENT_HYBRID_CLOUD_CONTROL_CLASS:
          Constructor<?> hybridCcloudProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class, Configuration.class);
          return (HybridCCloudAclsProvider)
              hybridCcloudProviderConstructor.newInstance(builderAdminClient, config);
        default:
          throw new IOException("Unknown access control provided. " + accessControlClassName);
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public BindingsBuilderProvider builder() throws IOException {
    String accessControlClass = config.getAccessControlClassName();

    try {
      if (accessControlClass.equalsIgnoreCase(ACCESS_CONTROL_DEFAULT_CLASS)) {
        return new AclsBindingsBuilder(config);
      } else if (accessControlClass.equalsIgnoreCase(CONFLUENT_CLOUD_CONTROL_CLASS)
          || accessControlClass.equalsIgnoreCase(CONFLUENT_HYBRID_CLOUD_CONTROL_CLASS)) {
        return new AclsBindingsBuilder(config);
      } else {
        throw new IOException(accessControlClass + " Unknown access control provided.");
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
}
