package com.purbon.kafka.topology;

import com.purbon.kafka.topology.model.JulieRoleAcl;
import com.purbon.kafka.topology.model.users.*;
import com.purbon.kafka.topology.model.users.platform.KsqlServerInstance;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public interface BindingsBuilderProvider {

  List<TopologyAclBinding> buildBindingsForConnect(Connector connector, String topicPrefix);

  List<TopologyAclBinding> buildBindingsForKStream(KStream stream, String topicPrefix);

  List<TopologyAclBinding> buildLiteralBindingsForConsumers(
      Collection<Consumer> consumers, String resource);

  List<TopologyAclBinding> buildPrefixedBindingsForConsumers(
      Collection<Consumer> consumers, String resource);

  List<TopologyAclBinding> buildLiteralBindingsForProducers(
      Collection<Producer> principals, String resource);

  List<TopologyAclBinding> buildPrefixedBindingsForProducers(
      Collection<Producer> principals, String resource);

  List<TopologyAclBinding> buildBindingsForSchemaRegistry(SchemaRegistryInstance schemaRegistry);

  default List<TopologyAclBinding> setSchemaAuthorization(
      String principal,
      List<String> subjects,
      String role,
      boolean prefixed,
      Boolean shouldOptimizeAcls,
      String namePrefix) {
    return Collections.emptyList();
  }

  default List<TopologyAclBinding> setConnectorAuthorization(
      String principal, List<String> connectors) {
    return Collections.emptyList();
  }

  Collection<TopologyAclBinding> buildBindingsForKSqlServer(KsqlServerInstance ksqlServer);

  Collection<TopologyAclBinding> buildBindingsForKSqlApp(KSqlApp app, String prefix);

  Collection<TopologyAclBinding> buildBindingsForJulieRole(
      Other other, String name, List<JulieRoleAcl> acls) throws IOException;

  // TODO: uncertain whether this should be part of this interface as it causes
  // it to be required in RBACBindingsBuilder as well as AclsBindingsBuilder
  Collection<TopologyAclBinding> buildBindingForMirrorMaker2(MirrorMaker2 mm2);
}
