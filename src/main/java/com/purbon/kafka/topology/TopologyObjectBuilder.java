package com.purbon.kafka.topology;

import com.purbon.kafka.topology.model.PlanMap;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.serdes.PlanMapSerdes;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TopologyObjectBuilder {

  private static final String PREFIX_SEPARATOR = ".";

  /**
   * Builds a unique key for a topology based on its full prefix. This includes the context and all
   * other namespace fields (like source, privacy, etc.) in their declaration order.
   *
   * @param topology The topology to build a key for
   * @return A string key representing the full namespace prefix
   */
  private static String buildProjectPrefix(Topology topology, Configuration config) {
    if (!config.isProjectNamespacingEnabled()) {
      return topology.getContext();
    }
    StringBuilder sb = new StringBuilder();
    sb.append(topology.getContext());
    Map<String, Object> fullContext = topology.asFullContext();
    for (String key : topology.getOrder()) {
      sb.append(PREFIX_SEPARATOR);
      Object value = fullContext.get(key);
      sb.append(value != null ? value.toString() : "");
    }
    return sb.toString();
  }

  public static Map<String, Topology> build(String fileOrDir) throws IOException {
    return build(fileOrDir, "", new Configuration());
  }

  public static Map<String, Topology> build(String fileOrDir, String plansFile) throws IOException {
    return build(fileOrDir, plansFile, new Configuration());
  }

  public static Map<String, Topology> build(String fileOrDir, Configuration config)
      throws IOException {
    return build(fileOrDir, "", config);
  }

  public static Map<String, Topology> build(
      String fileOrDir, String plansFile, Configuration config) throws IOException {
    PlanMap plans = buildPlans(plansFile);
    List<Topology> topologies = parseListOfTopologies(fileOrDir, config, plans);
    Map<String, Topology> collection = new HashMap<>();
    for (Topology topology : topologies) {
      String projectPrefix = buildProjectPrefix(topology, config);
      String context = topology.getContext();
      // Check if we're seeing a new context (not just a new prefix within same context)
      boolean hasNewContext =
          collection.keySet().stream()
              .noneMatch(key -> key.equals(context) || key.startsWith(context + PREFIX_SEPARATOR));
      if (!config.areMultipleContextPerDirEnabled() && (hasNewContext && collection.size() >= 1)) {
        // the parsing found a new topology with a different context, as it is not enabled
        // it should be flagged as error
        throw new IOException("Topologies from different contexts are not allowed");
      }
      if (!collection.containsKey(projectPrefix)) {
        collection.put(projectPrefix, topology);
      } else {
        Topology mainTopology = collection.get(projectPrefix);
        List<String> projectNames =
            mainTopology.getProjects().stream().map(p -> p.getName().toLowerCase()).toList();
        for (Project project : topology.getProjects()) {
          if (projectNames.contains(project.getName().toLowerCase())) {
            throw new IOException(
                "Trying to add a project with name "
                    + project.getName()
                    + " in a sub topology (prefix: "
                    + projectPrefix
                    + ") that already contain the same project. Merging projects is not yet supported");
          }
          mainTopology.addProject(project);
        }
        for (String other : topology.getOrder()) {
          var topologyContext = topology.asFullContext();
          if (!mainTopology.getOrder().contains(other)) {
            String value = String.valueOf(topologyContext.get(other));
            mainTopology.addOther(other, value);
          }
        }
        var existingSpecial = mainTopology.getSpecialTopics().stream().map(Topic::getName).toList();
        for (Topic specialTopic : topology.getSpecialTopics()) {
          if (existingSpecial.contains(specialTopic.getName())) {
            throw new IOException(
                "Trying to add special_topic with name "
                    + specialTopic.getName()
                    + " in a topology (prefix: "
                    + projectPrefix
                    + "). Each special topic should only be"
                    + " defined once.");
          }
          mainTopology.addSpecialTopic(specialTopic);
        }
        collection.put(projectPrefix, mainTopology);
      }
    }
    return collection;
  }

  private static PlanMap buildPlans(String plansFile) throws IOException {
    PlanMapSerdes plansSerdes = new PlanMapSerdes();
    return plansFile.isEmpty() ? new PlanMap() : plansSerdes.deserialise(new File(plansFile));
  }

  private static List<Topology> parseListOfTopologies(
      String fileOrDir, Configuration config, PlanMap plans) {
    TopologySerdes parser = new TopologySerdes(config, plans);
    List<Topology> topologies = new ArrayList<>();
    final Path path = Paths.get(fileOrDir);
    if (Files.isDirectory(path)) {
      loadFromDirectory(path, config.isRecursive(), parser, topologies);
    } else {
      topologies.add(parser.deserialise(new File(fileOrDir)));
    }
    return topologies;
  }

  private static void loadFromDirectory(
      final Path directory,
      final boolean recursive,
      final TopologySerdes parser,
      final List<Topology> topologies) {
    try {
      Files.list(directory)
          .sorted()
          .filter(p -> !Files.isDirectory(p))
          .map(path -> parser.deserialise(path.toFile()))
          .forEach(topologies::add);
      if (recursive) {
        Files.list(directory)
            .sorted()
            .filter(Files::isDirectory)
            .forEach(p -> loadFromDirectory(p, recursive, parser, topologies));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
