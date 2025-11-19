package com.purbon.kafka.topology;

import static java.lang.System.exit;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.*;
import org.apache.commons.cli.help.HelpFormatter;

public class CommandLineInterface {

  public static final String TOPOLOGY_OPTION = "topology";
  public static final String TOPOLOGY_DESC = "Topology config file.";

  public static final String PLANS_OPTION = "plans";
  public static final String PLANS_DESC = "File describing the predefined plans";

  public static final String BROKERS_OPTION = "brokers";
  public static final String BROKERS_DESC = "The Apache Kafka server(s) to connect to.";

  public static final String CLIENT_CONFIG_OPTION = "clientConfig";
  public static final String CLIENT_CONFIG_DESC = "The client configuration file.";

  public static final String OVERRIDING_CLIENT_CONFIG_OPTION = "overridingClientConfig";
  public static final String OVERRIDING_CLIENT_CONFIG_DESC =
      "The overriding AdminClient configuration file.";

  public static final String DRY_RUN_OPTION = "dryRun";
  public static final String DRY_RUN_DESC = "Print the execution plan without altering anything.";

  public static final String RECURSIVE_OPTION = "recursive";
  public static final String RECURSIVE_DESC =
      "Recursively look for topology files below the given directory.";

  public static final String QUIET_OPTION = "quiet";
  public static final String QUIET_DESC = "Print minimum status update";

  public static final String DONT_WARN_FOR_READ_ONLY_STREAMS_OPTION = "accept-read-only-streams";
  public static final String DONT_WARN_FOR_READ_ONLY_STREAMS_DESC =
      "Don't warn for streams that only have readers. Use this if you abuse streams to have a lazy person's consumers with state.";

  public static final String DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_OPTION =
      "accept-projects-without-topics";
  public static final String DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_DESC =
      "Don't warn for projects without topics.";

  public static final String VALIDATE_OPTION = "validate";
  public static final String VALIDATE_DESC = "Only run configured validations in your topology";

  public static final String HELP_OPTION = "help";
  public static final String HELP_DESC = "Prints usage information.";

  public static final String VERSION_OPTION = "version";
  public static final String VERSION_DESC = "Prints useful version information.";

  public static final String APP_NAME = "julie-ops";

  private HelpFormatter formatter;
  private CommandLineParser parser;
  private Options options;

  public CommandLineInterface() {
    formatter = HelpFormatter.builder().setShowSince(false).get();
    parser = new DefaultParser();
    options = buildOptions();
  }

  private Options buildOptions() {
    final Options options = new Options();
    options.addOption(requiredArgOption(TOPOLOGY_OPTION, TOPOLOGY_DESC));
    options.addOption(argOption(PLANS_OPTION, PLANS_DESC));
    options.addOption(argOption(BROKERS_OPTION, BROKERS_DESC));
    options.addOption(requiredArgOption(CLIENT_CONFIG_OPTION, CLIENT_CONFIG_DESC));
    options.addOption(argOption(OVERRIDING_CLIENT_CONFIG_OPTION, OVERRIDING_CLIENT_CONFIG_DESC));
    options.addOption(noArgOption(DRY_RUN_OPTION, DRY_RUN_DESC));
    options.addOption(noArgOption(RECURSIVE_OPTION, RECURSIVE_DESC));
    options.addOption(
        noArgOption(DONT_WARN_FOR_READ_ONLY_STREAMS_OPTION, DONT_WARN_FOR_READ_ONLY_STREAMS_DESC));
    options.addOption(
        noArgOption(
            DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_OPTION,
            DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_DESC));
    options.addOption(noArgOption(QUIET_OPTION, QUIET_DESC));
    options.addOption(noArgOption(VALIDATE_OPTION, VALIDATE_DESC));
    options.addOption(noArgOption(VERSION_OPTION, VERSION_DESC));
    options.addOption(noArgOption(HELP_OPTION, HELP_DESC));
    return options;
  }

  private Option argOption(final String longOption, final String description) {
    return Option.builder().longOpt(longOption).hasArg().desc(description).required(false).get();
  }

  private Option requiredArgOption(final String longOption, final String description) {
    return Option.builder().longOpt(longOption).hasArg().desc(description).required().get();
  }

  private Option noArgOption(final String longOption, final String description) {
    return Option.builder()
        .longOpt(longOption)
        .hasArg(false)
        .desc(description)
        .required(false)
        .get();
  }

  public static void main(String[] args) {
    CommandLineInterface cli = new CommandLineInterface();
    try {
      cli.run(args);
      exit(0);
    } catch (Exception ex) {
      ex.printStackTrace();
      exit(1);
    }
  }

  public void run(String[] args) throws Exception {
    printHelpOrVersion(args);
    CommandLine cmd = parseArgsOrExit(args);
    Map<String, String> config = parseConfig(cmd);
    processTopology(
        cmd.getOptionValue(TOPOLOGY_OPTION), cmd.getOptionValue(PLANS_OPTION, "default"), config);
    if (!cmd.hasOption(DRY_RUN_OPTION) && !cmd.hasOption(VALIDATE_OPTION)) {
      System.out.println("Kafka Topology updated");
    }
  }

  private Map<String, String> parseConfig(CommandLine cmd) {
    Map<String, String> config = new HashMap<>();
    if (cmd.hasOption(BROKERS_OPTION)) {
      config.put(BROKERS_OPTION, cmd.getOptionValue(BROKERS_OPTION));
    }
    config.put(DRY_RUN_OPTION, String.valueOf(cmd.hasOption(DRY_RUN_OPTION)));
    config.put(RECURSIVE_OPTION, String.valueOf(cmd.hasOption(RECURSIVE_OPTION)));
    config.put(
        DONT_WARN_FOR_READ_ONLY_STREAMS_OPTION,
        String.valueOf(cmd.hasOption(DONT_WARN_FOR_READ_ONLY_STREAMS_OPTION)));
    config.put(
        DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_OPTION,
        String.valueOf(cmd.hasOption(DONT_WARN_FOR_PROJECTS_WITHOUT_TOPICS_OPTION)));
    config.put(QUIET_OPTION, String.valueOf(cmd.hasOption(QUIET_OPTION)));
    config.put(VALIDATE_OPTION, String.valueOf(cmd.hasOption(VALIDATE_OPTION)));
    config.put(
        OVERRIDING_CLIENT_CONFIG_OPTION, cmd.getOptionValue(OVERRIDING_CLIENT_CONFIG_OPTION));
    config.put(CLIENT_CONFIG_OPTION, cmd.getOptionValue(CLIENT_CONFIG_OPTION));
    return config;
  }

  private void printHelpOrVersion(String[] args) {
    List<String> listOfArgs = Arrays.asList(args);
    if (listOfArgs.contains("--" + HELP_OPTION)) {
      printHelp();
      exit(0);
    } else if (listOfArgs.contains("--" + VERSION_OPTION)) {
      System.out.println(JulieOps.getVersion());
      exit(0);
    }
  }

  private CommandLine parseArgsOrExit(String[] args) {
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Parsing failed cause of " + e.getMessage());
      printHelp();
      exit(1);
    }
    return cmd;
  }

  private void printHelp() {
    try {
      formatter.printHelp(APP_NAME, "", options, "", false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void processTopology(String topologyFile, String plansFile, Map<String, String> config)
      throws Exception {
    try (JulieOps builder = JulieOps.build(topologyFile, plansFile, config)) {
      builder.run();
    }
  }
}
