package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.RECURSIVE_OPTION;
import static com.purbon.kafka.topology.Constants.JULIE_ENABLE_MULTIPLE_CONTEXT_PER_DIR;
import static com.purbon.kafka.topology.Constants.JULIE_PROJECT_NAMESPACE_ENABLED;
import static com.purbon.kafka.topology.Constants.PLATFORM_SERVERS_CONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.exceptions.TopologyParsingException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.Test;

public class TopologyObjectBuilderTest {

  @Test
  public void buildTopicNameTest() throws IOException {
    String fileOrDirPath = TestUtils.getResourceFilename("/dir");
    var map = TopologyObjectBuilder.build(fileOrDirPath);
    assertThat(map).hasSize(1);
    for (var entry : map.entrySet()) {
      assertThat(entry.getValue().getProjects()).hasSize(5);
    }
  }

  @Test
  public void buildTopicNameTest_withNamespaces() throws IOException {
    String fileOrDirPath = TestUtils.getResourceFilename("/dir");
    var props = new Properties();
    props.put(JULIE_PROJECT_NAMESPACE_ENABLED, "true");
    Configuration config = new Configuration(new HashMap<>(), props);
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    // With prefix-based grouping, /dir has 3 files with 3 different prefixes:
    // - contextOrg.source (descriptor.yaml with 2 projects: foo, bar)
    // - contextOrg.source.foo.bar.zet (descriptor-with-others.yml with 2 projects: zet, bear)
    // - contextOrg.source.external (descriptor-with-special.yml with 1 project: aaa)
    assertThat(map).hasSize(3);
    int totalProjects = map.values().stream().mapToInt(t -> t.getProjects().size()).sum();
    assertThat(totalProjects).isEqualTo(5);
  }

  @Test
  public void buildOutOfMultipleTopos() throws IOException {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    var props = new Properties();
    props.put(JULIE_ENABLE_MULTIPLE_CONTEXT_PER_DIR, "true");
    Configuration config = new Configuration(cliOps, props);
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_multiple");
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    assertThat(map).hasSize(2);
    for (var entry : map.entrySet()) {
      assertThat(entry.getValue().getProjects()).hasSize(4);
    }
    var contextTopology = map.get("contextOrg");
    assertThat(contextTopology.getOrder().get(0)).isEqualTo("source");
    assertThat(contextTopology.getOrder().get(1)).isEqualTo("foo");
    assertThat(contextTopology.getOrder().get(2)).isEqualTo("bar");
    assertThat(contextTopology.getOrder().get(3)).isEqualTo("zet");
    var context = map.get("context2");
    assertThat(context.getOrder()).hasSize(3);
    assertThat(context.getOrder().get(0)).isEqualTo("source2");
    assertThat(context.getOrder().get(1)).isEqualTo("foo2");
    assertThat(context.getOrder().get(2)).isEqualTo("bar2");
    var projects =
        Arrays.asList(
            "context2.source2.foo.bar.projectFoo.",
            "context2.source2.foo.bar.projectBar.",
            "context2.source2.foo.bar.projectZet.",
            "context2.source2.foo.bar.projectBear.");
    assertThat(context.getProjects()).hasSize(4);
    for (Project proj : context.getProjects()) {
      assertThat(projects).contains(proj.namePrefix());
    }
  }

  @Test
  public void buildOutOfMultipleTopos_withNamespaces() throws IOException {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    var props = new Properties();
    props.put(JULIE_ENABLE_MULTIPLE_CONTEXT_PER_DIR, "true");
    props.put(JULIE_PROJECT_NAMESPACE_ENABLED, "true");
    Configuration config = new Configuration(cliOps, props);
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_multiple");
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    // With the new prefix-based grouping, files with different prefixes are separate topologies
    // dir_with_multiple has 4 files with 4 different prefixes:
    // - context2.source2.foo.bar (0-descriptor-context2.yml)
    // - context2.source2 (1-descriptor-context2.yaml)
    // - contextOrg.source (1-descriptor.yaml)
    // - contextOrg.source.foo.bar.zet (2-descriptor.yml)
    assertThat(map).hasSize(4);
    // Verify context2.source2.foo.bar topology
    var context2FooBar = map.get("context2.source2.foo.bar");
    assertThat(context2FooBar).isNotNull();
    assertThat(context2FooBar.getOrder()).hasSize(3);
    assertThat(context2FooBar.getOrder().get(0)).isEqualTo("source2");
    assertThat(context2FooBar.getOrder().get(1)).isEqualTo("foo2");
    assertThat(context2FooBar.getOrder().get(2)).isEqualTo("bar2");
    assertThat(context2FooBar.getProjects()).hasSize(2);
    var projectsFooBar =
        Arrays.asList(
            "context2.source2.foo.bar.projectFoo.", "context2.source2.foo.bar.projectBar.");
    for (Project proj : context2FooBar.getProjects()) {
      assertThat(projectsFooBar).contains(proj.namePrefix());
    }
    // Verify context2.source2 topology
    var context2Source = map.get("context2.source2");
    assertThat(context2Source).isNotNull();
    assertThat(context2Source.getOrder()).hasSize(1);
    assertThat(context2Source.getOrder().getFirst()).isEqualTo("source2");
    assertThat(context2Source.getProjects()).hasSize(2);
  }

  @Test(expected = IOException.class)
  public void buildOutOfMultipleToposIfNotEnabled() throws IOException {
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_multiple");
    TopologyObjectBuilder.build(fileOrDirPath);
  }

  @Test(expected = IOException.class)
  public void shouldRaiseAnExceptionIfTryingToParseMultipleTopologiesWithSharedProjects()
      throws IOException {
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_prob");
    TopologyObjectBuilder.build(fileOrDirPath);
  }

  @Test(expected = IOException.class)
  public void shouldRaiseAnExceptionWhenThereIsActualCollectionWithNamespacesEnabled()
      throws IOException {
    var props = new Properties();
    props.put(JULIE_PROJECT_NAMESPACE_ENABLED, "true");
    Configuration config = new Configuration(new HashMap<>(), props);
    // This directory has two files with the SAME prefix (ctx.src.public) but same project name
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_actual_collision");
    TopologyObjectBuilder.build(fileOrDirPath, config);
  }

  @Test
  public void shouldAllowSameProjectNameWithDifferentPrefix() throws IOException {
    // This directory has two files with the same context but different privacy values
    // (different full prefix), so projects with the same name should be allowed
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_different_prefix");
    var props = new Properties();
    props.put(JULIE_PROJECT_NAMESPACE_ENABLED, "true");
    Configuration config = new Configuration(new HashMap<>(), props);
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    // Should produce two separate topologies keyed by their full prefix
    assertThat(map).hasSize(2);
    // One topology for ctx.src.public, another for ctx.src.internal
    assertThat(map).containsKey("ctx.src.public");
    assertThat(map).containsKey("ctx.src.internal");
    // Each should have one project named "prj"
    for (var entry : map.entrySet()) {
      assertThat(entry.getValue().getProjects()).hasSize(1);
      assertThat(entry.getValue().getProjects().getFirst().getName()).isEqualTo("prj");
    }
  }

  @Test
  public void shouldMergeProjectsFromFilesWithSamePrefix() throws IOException {
    // This directory has two files with the SAME prefix (ctx.src.public)
    // but different project names.
    // They should be merged into a single topology with 2 projects
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_same_prefix");
    var props = new Properties();
    props.put(JULIE_PROJECT_NAMESPACE_ENABLED, "true");
    Configuration config = new Configuration(new HashMap<>(), props);
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    assertThat(map).hasSize(1);
    assertThat(map).containsKey("ctx.src.public");
    var topology = map.get("ctx.src.public");
    assertThat(topology.getProjects()).hasSize(2);
    var projectNames = topology.getProjects().stream().map(Project::getName).toList();
    assertThat(projectNames).containsExactlyInAnyOrder("projectAlpha", "projectBeta");
  }

  @Test
  public void buildOnlyConnectorTopo() throws IOException {
    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    Properties props = new Properties();
    props.put(PLATFORM_SERVERS_CONNECT + ".0", "connector0:http://foo:8083");
    Configuration config = new Configuration(cliOps, props);
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor-only-conn.yaml");
    var topologies = TopologyObjectBuilder.build(fileOrDirPath, config);
    var topology = topologies.values().stream().findFirst().get();
    var proj = topology.getProjects().getFirst();
    assertThat(proj.getConnectorArtefacts().getConnectors()).hasSize(2);
  }

  @Test
  public void testConfigUpdateWhenUsingCustomPlans() throws IOException {
    String descriptorFile = TestUtils.getResourceFilename("/descriptor-with-plans.yaml");
    String plansFile = TestUtils.getResourceFilename("/plans.yaml");
    Map<String, Topology> topologies = TopologyObjectBuilder.build(descriptorFile, plansFile);
    var topology = topologies.values().stream().findFirst().get();
    assertThat(topology).isNotNull();
    List<Topic> topics = topology.getProjects().getFirst().getTopics();
    Map<String, Topic> map =
        topics.stream().collect(Collectors.toMap(Topic::getName, topic -> topic));
    // should include the config values from the plan into the topic config.
    Topic topic = map.get("fooBar");
    Map<String, String> config = new HashMap<>();
    config.put("foo", "bar");
    config.put("bar", "3");
    assertThat(topic.getConfig()).containsAllEntriesOf(config);
    // should respect values from the original config if not present in the plan description
    topic = map.get("barFoo");
    assertThat(topic.getConfig()).containsEntry("replication.factor", "1");
    // should override values with priority given to the plans
    topic = map.get("barFooBar");
    assertThat(topic.getConfig()).containsEntry("replication.factor", "1");
    assertThat(topic.getConfig()).containsEntry("bar", "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopologyWithPlansButWithNoPlansDef() throws IOException {
    String descriptorFile = TestUtils.getResourceFilename("/descriptor-with-plans.yaml");
    TopologyObjectBuilder.build(descriptorFile);
  }

  @Test(expected = TopologyParsingException.class)
  public void testTopologyWithInvalidPlan() throws IOException {
    String descriptorFile = TestUtils.getResourceFilename("/descriptor-with-invalid-plan.yaml");
    String plansFile = TestUtils.getResourceFilename("/plans.yaml");
    TopologyObjectBuilder.build(descriptorFile, plansFile);
  }

  @Test(expected = TopologyParsingException.class)
  public void testInvalidTopology() throws IOException {
    String descriptorFile =
        TestUtils.getResourceFilename("/errors_dir/descriptor-with-errors.yaml");
    TopologyObjectBuilder.build(descriptorFile);
  }

  @Test(expected = TopologyParsingException.class)
  public void testInvalidTopologyFromDir() throws IOException {
    String dirPath = TestUtils.getResourceFilename("/errors_dir");
    TopologyObjectBuilder.build(dirPath);
  }

  @Test
  public void shouldReadFilesRecursively() throws IOException {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(RECURSIVE_OPTION, "true");
    var props = new Properties();
    Configuration config = new Configuration(cliOps, props);
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_recursive");
    var map = TopologyObjectBuilder.build(fileOrDirPath, config);
    assertThat(map).hasSize(1);
    final Topology topology = map.values().iterator().next();
    assertThat(topology.getProjects()).hasSize(4);
  }
}
