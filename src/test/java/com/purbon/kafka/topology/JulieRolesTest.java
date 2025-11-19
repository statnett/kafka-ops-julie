package com.purbon.kafka.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.model.*;
import com.purbon.kafka.topology.model.users.MirrorMaker2;
import com.purbon.kafka.topology.model.users.Other;
import com.purbon.kafka.topology.serdes.JulieRolesSerdes;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import com.purbon.kafka.topology.utils.JinjaUtils;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JulieRolesTest {

  JulieRolesSerdes parser;

  @Before
  public void before() {
    this.parser = new JulieRolesSerdes();
  }

  @Test
  public void testSerdes() throws IOException {
    JulieRoles roles = parser.deserialise(TestUtils.getResourceFile("/roles-rbac.yaml"));

    assertThat(roles.getRoles()).hasSize(2);
    for (JulieRole role : roles.getRoles()) {
      assertThat(role.getName()).isIn("app", "other");
    }

    JulieRole role = roles.get("app");
    List<String> resources =
        role.getAcls().stream().map(JulieRoleAcl::getResourceType).collect(Collectors.toList());
    assertThat(resources).contains("Topic", "Group", "Subject", "Connector");

    assertThat(role.getName()).isEqualTo("app");
    assertThat(role.getAcls()).hasSize(9);
    assertThat(role.getAcls().get(0).getRole()).isEqualTo("ResourceOwner");

    role = roles.get("other");
    resources =
        role.getAcls().stream().map(JulieRoleAcl::getResourceType).collect(Collectors.toList());
    assertThat(resources).contains("Topic");
    assertThat(role.getName()).isEqualTo("other");
    assertThat(role.getAcls()).hasSize(2);

    TopologySerdes topologySerdes =
        new TopologySerdes(new Configuration(), TopologySerdes.FileType.YAML, new PlanMap());
    Topology topology = topologySerdes.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));

    var project = topology.getProjects().get(0);
    for (Map.Entry<String, List<Other>> entry : project.getOthers().entrySet()) {
      if (!entry.getKey().equals("app")) {
        continue;
      }
      role = roles.get(entry.getKey());
      var other = entry.getValue().get(0);
      var acls =
          role.getAcls().stream()
              .map(
                  acl -> {
                    String resourceName =
                        JinjaUtils.serialise(acl.getResourceName(), other.asMap());
                    return new JulieRoleAcl(
                        acl.getResourceType(),
                        resourceName,
                        acl.getPatternType(),
                        acl.getHost(),
                        acl.getOperation(),
                        acl.getPermissionType());
                  })
              .toList();
      var names = acls.stream().map(JulieRoleAcl::getResourceName).collect(Collectors.toList());
      assertThat(names).contains("test.subject", "con");
    }
  }

  @Test
  public void testSerdesWithRoleDefault() throws IOException {
    JulieRoles roles = parser.deserialise(TestUtils.getResourceFile("/roles-mirrormaker.yaml"));
    TopologySerdes topologySerdes =
        new TopologySerdes(new Configuration(), TopologySerdes.FileType.YAML, new PlanMap());
    Topology topology =
        topologySerdes.deserialise(
            TestUtils.getResourceFile("/descriptor-mirrormaker-source.yaml"));

    var project = topology.getProjects().get(0);
    for (Map.Entry<String, List<Other>> entry : project.getOthers().entrySet()) {
      if (!entry.getKey().equals("app")) {
        continue;
      }
      var role = roles.get(entry.getKey());
      var other = entry.getValue().get(0);
      var acls =
          role.getAcls().stream()
              .map(
                  acl -> {
                    String resourceName =
                        JinjaUtils.serialise(acl.getResourceName(), other.asMap());
                    return new JulieRoleAcl(
                        acl.getResourceType(),
                        resourceName,
                        acl.getPatternType(),
                        acl.getHost(),
                        acl.getOperation(),
                        acl.getPermissionType());
                  })
              .toList();
      var names = acls.stream().map(JulieRoleAcl::getResourceName).toList();

      var expected =
          new String[] {
            "test-cluster-status",
            "test-cluster-offsets",
            "test-cluster-configs",
            "target-prefix.",
            "mm2-offset-syncs.test-mm.internal",
            "test-mm.checkpoints.internal",
            "mirrormaker2-heartbeat"
          };

      Assert.assertEquals(expected.length, names.size());

      for (String t : expected) {
        Assert.assertTrue(names.contains(t));
      }
    }
  }

  @Test(expected = IOException.class)
  public void testTopologyValidationException() throws IOException {
    JulieRoles roles = parser.deserialise(TestUtils.getResourceFile("/roles.yaml"));
    TopologySerdes topologySerdes = new TopologySerdes();

    Topology topology = topologySerdes.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    roles.validateTopology(topology);
  }

  @Test
  public void testTopologyValidationCorrect() throws IOException {
    JulieRoles roles = parser.deserialise(TestUtils.getResourceFile("/roles-goodTest.yaml"));
    TopologySerdes topologySerdes = new TopologySerdes();

    Topology topology = topologySerdes.deserialise(TestUtils.getResourceFile("/descriptor.yaml"));
    roles.validateTopology(topology);
  }

  @Test
  public void testTopologyMerge() throws IOException {
    JulieRoles roles1 = parser.deserialise(TestUtils.getResourceFile("/roles.yaml"));
    JulieRoles roles2 = parser.deserialise(TestUtils.getResourceFile("/roles2.yaml"));

    JulieRoles roles = roles1.merge(roles2);

    assert roles.get("app") != null;
    assert roles.get("app2") != null;
    assert roles.get("other") != null;
  }

  @Test
  public void testMirrorMakerRole() throws IOException {
    JulieRoles mmRoles = parser.deserialise(TestUtils.getResourceFile("/roles-mirrormaker.yaml"));
    JulieRoles topicRoles =
        parser.deserialise(TestUtils.getResourceFile("/roles-with-default-allow.yaml"));
    JulieRoles roles = mmRoles.merge(topicRoles);

    TopologySerdes topologySerdesSource = new TopologySerdes();
    TopologySerdes topologySerdesTarget = new TopologySerdes();

    Topology sourceTopology =
        topologySerdesSource.deserialise(
            TestUtils.getResourceFile("/descriptor-mirrormaker-source.yaml"));
    Topology targetTopology =
        topologySerdesTarget.deserialise(
            TestUtils.getResourceFile("/descriptor-mirrormaker-target.yaml"));

    roles.validateTopology(sourceTopology);
    roles.validateTopology(targetTopology);

    var expectedSource =
        new String[] {
          "test-cluster-status",
          "test-cluster-offsets",
          "test-cluster-configs",
          "source-topic-A",
          "source-topic-B"
        };

    var expectedTarget =
        new String[] {
          "test-cluster-status",
          "test-cluster-offsets",
          "test-cluster-configs",
          "target-prefix.",
          "mm2-offset-syncs.test-mm.internal",
          "test-mm.checkpoints.internal",
          "source-topic-A",
          "source-topic-B"
        };

    MirrorMaker2 mirrorMaker = sourceTopology.getProjects().get(0).getMirrorMakers().get(0);
    List<Topic> sourceTopics = sourceTopology.getProjects().get(0).getTopics();

    Collection<Object> topics = Collections.singleton(mirrorMaker.getAllTopics());
    topics.addAll(sourceTopics);

    for (String t : expectedSource) {
      Assert.assertTrue(topics.contains(t));
    }
  }

  @Test
  public void testRoleAndSpecialTopicsValidationSucceeds() throws IOException {
    JulieRoles roles1 = parser.deserialise(TestUtils.getResourceFile("/roles-mirrormaker.yaml"));
    JulieRoles roles2 = parser.deserialise(TestUtils.getResourceFile("/roles2.yaml"));
    JulieRoles roles = roles1.merge(roles2);

    TopologySerdes topologySerdes = new TopologySerdes();

    Topology topology =
        topologySerdes.deserialise(
            TestUtils.getResourceFile("/descriptor-with-special-topics-and-roles.yaml"));
    roles.validateTopology(topology);
  }

  @Test
  public void testDefaultAllowPerssionTypeInRole() throws IOException {
    JulieRoles roles =
        parser.deserialise(TestUtils.getResourceFile("/roles-with-default-allow.yaml"));

    var role = roles.get("defaultAllow");

    Assert.assertEquals("ALLOW", role.getAcls().getFirst().getPermissionType());
  }
}
