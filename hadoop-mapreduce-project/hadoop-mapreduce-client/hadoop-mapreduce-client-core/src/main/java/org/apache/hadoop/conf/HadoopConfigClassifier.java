package org.apache.hadoop.conf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.mapreduce.util.ConfigUtil;

public class HadoopConfigClassifier {

  private static final String CORE_DEFAULT_XML = "core-default.xml";
  private static final String MAPRED_DEFAULT_XML = "mapred-default.xml";
  private static final String YARN_DEFAULT_XML = "yarn-default.xml";

  private static final String CORE_DEFAULT_PROPERTIES = CoreDefaultProperties.class.getName();
  private static final String MAPRED_DEFAULT_PROPERTIES = "org.apache.hadoop.mapreduce.conf.MapReduceDefaultProperties";
  private static final String YARN_DEFAULT_PROPERTIES = "org.apache.hadoop.yarn.conf.YarnDefaultProperties";

  private static final String CORE_SITE_XML = "core-site.xml";
  private static final String MAPRED_SITE_XML = "mapred-site.xml";
  private static final String YARN_SITE_XML = "yarn-site.xml";

  private static Map<String, ConfigOption> OPTIONS = new HashMap<>();
  public static final int PADDING = 40;

  static {
    for (ConfigOption option : ConfigOption.values()) {
      OPTIONS.put(option.option, option);
    }
  }

  enum Operation {
    FIND_MAPR_MODIFIED_PROPERTIES,
    FIND_MAPR_ADDED_PROPERTIES
  }

  enum ConfigOption {

    /*** APACHE's {core,mapred,yarn}-default.xml ***/
    APACHE_CORE_DEFAULT(
        "print-apache-core-default-xml",
        "Prints properties defined in Apache Hadoop's core-default.xml.",
        new String[]{CORE_DEFAULT_XML},
        null, null),

    APACHE_MAPRED_DEFAULT(
        "print-apache-mapred-default-xml",
        "Prints properties defined in Apache Hadoop's mapred-default.xml.",
        new String[]{MAPRED_DEFAULT_XML},
        null, null),

    APACHE_YARN_DEFAULT(
        "print-apache-yarn-default-xml",
        "Prints properties defined in Apache Hadoop's yarn-default.xml.",
        new String[]{YARN_DEFAULT_XML},
        null, null),


    /*** MapR's modifications of Apache's {core,mapred,yarn}-default.xml ***/
    MAPR_MODIFIED_CORE_DEFAULT(
        "print-mapr-modified-core-properties",
        "Prints MapR's modifications of properties defined in Apache Hadoop's core-default.xml.",
        new String[]{CORE_DEFAULT_PROPERTIES},
        APACHE_CORE_DEFAULT, Operation.FIND_MAPR_MODIFIED_PROPERTIES),

    MAPR_MODIFIED_MAPRED_DEFAULT(
        "print-mapr-modified-mapred-properties",
        "Prints MapR's modifications of properties defined in Apache Hadoop's mapred-default.xml.",
        new String[]{MAPRED_DEFAULT_PROPERTIES},
        APACHE_MAPRED_DEFAULT, Operation.FIND_MAPR_MODIFIED_PROPERTIES),

    MAPR_MODIFIED_YARN_DEFAULT(
        "print-mapr-modified-yarn-properties",
        "Prints MapR's modifications of properties defined in Apache Hadoop's yarn-default.xml.",
        new String[]{YARN_DEFAULT_PROPERTIES},
        APACHE_YARN_DEFAULT, Operation.FIND_MAPR_MODIFIED_PROPERTIES),


    /*** MapR's additions to Apache's {core,mapred,yarn}-default.xml ***/
    MAPR_ADDED_CORE_DEFAULT(
        "print-mapr-added-core-properties",
        "Prints MapR's additions to properties defined in Apache Hadoop's core-default.xml.",
        new String[]{CORE_DEFAULT_PROPERTIES},
        APACHE_CORE_DEFAULT, Operation.FIND_MAPR_ADDED_PROPERTIES),

    MAPR_ADDED_MAPRED_DEFAULT(
        "print-mapr-added-mapred-properties",
        "Prints MapR's additions to properties defined in Apache Hadoop's mapred-default.xml.",
        new String[]{MAPRED_DEFAULT_PROPERTIES},
        APACHE_MAPRED_DEFAULT, Operation.FIND_MAPR_ADDED_PROPERTIES),

    MAPR_ADDED_YARN_DEFAULT(
        "print-mapr-added-yarn-properties",
        "Prints MapR's additions to properties defined in Apache Hadoop's yarn-default.xml.",
        new String[]{YARN_DEFAULT_PROPERTIES},
        APACHE_YARN_DEFAULT, Operation.FIND_MAPR_ADDED_PROPERTIES),


    /*** {core,mapred,yarn}-site.xml ***/
    CORE_SITE(
        "print-core-site-xml",
        "Prints properties as defined in core-site.xml on this box.",
        new String[]{CORE_SITE_XML},
        null, null),

    MAPRED_SITE(
        "print-mapred-site-xml",
        "Prints properties as defined in yarn-site.xml on this box.",
        new String[]{MAPRED_SITE_XML},
        null, null),

    YARN_SITE(
        "print-yarn-site-xml",
        "Prints properties as defined in yarn-site.xml on this box.",
        new String[]{YARN_SITE_XML},
        null, null),


    /*** Effective {core,mapred,yarn} properties ***/
    EFFECTIVE_CORE(
        "print-effective-core-properties",
        "Prints effective values of 'core' properties. The effective values are obtained by{}" +
        "superimposing admin modified 'core' properties (in core-site.xml) and MapR added/modified{}" +
        "'core' properties (in MapR binaries) on top of Apache Hadoop's core-default.xml.{} ",
        new String[]{CORE_DEFAULT_XML, CORE_DEFAULT_PROPERTIES},
        null, null),

    EFFECTIVE_MAPRED(
        "print-effective-mapred-properties",
        "Prints effective values of 'mapred' properties. The effective values are obtained by{}" +
        "superimposing admin modified 'mapred' properties (in mapred-site.xml)and MapR added/modified{}" +
        "'mapred' properties (in MapR binaries) on top of Apache Hadoop's mapred-default.xml.{} ",
        new String[]{MAPRED_DEFAULT_XML, MAPRED_DEFAULT_PROPERTIES},
        null, null),

    EFFECTIVE_YARN(
        "print-effective-yarn-properties",
        "Prints effective values of 'yarn' properties. The effective values are obtained by{}" +
        "superimposing admin modified 'yarn' properties (in yarn-site.xml)and MapR added/modified{}" +
        "'yarn' properties (in MapR binaries) on top of Apache Hadoop's yarn-default.xml.",
        new String[]{YARN_DEFAULT_XML, YARN_DEFAULT_PROPERTIES},
        null, null),


    /*** Everything ***/
    ALL(
        "print-all-effective-properties",
        "Prints effective values of 'core,mapred,yarn' properties. The effective values{}" +
        "are obtained by superimposing admin modified {core,mapred,yarn} properties{}" +
        "(in *-site.xml) and MapR added/modified {core,mapred,yarn} properties (in MapR binaries){}" +
        "on top of Apache Hadoop's {core,mapred,yarn}-default.xml.",
        new String[]{
            CORE_DEFAULT_XML, CORE_DEFAULT_PROPERTIES, CORE_SITE_XML,
            MAPRED_DEFAULT_XML, MAPRED_DEFAULT_PROPERTIES, MAPRED_SITE_XML,
            YARN_DEFAULT_XML, YARN_DEFAULT_PROPERTIES, YARN_SITE_XML
        },
        null, null);
    private String option;
    private String help;
    private String[] sources;
    private ConfigOption other;
    private Operation operation;

    ConfigOption(String option, String help, String[] sources, ConfigOption other, Operation operation) {
      this.option = option;
      this.help = help;
      this.sources = sources;
      this.other = other;
      this.operation = operation;
    }
  }

  private static String getPadding(ConfigOption option) {
    int padding = PADDING - option.option.length();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < padding; i++) {
      builder.append(" ");
    }
    return builder.toString();
  }

  private static String indent(String line) {
    String[] splits = line.split("\\{\\}");
    if (splits.length <= 1) {
      return line;
    }
    StringBuilder builder = new StringBuilder();

    for (int j = 0; j < splits.length - 1; j++) {
      builder.append(splits[j]);
      builder.append("\n");
      for (int i = 0; i < PADDING; i++) {
        builder.append(" ");
      }
    }
    builder.append(splits[splits.length - 1]);
    return builder.toString();
  }

  private static void printHelp() {
    StringBuilder builder = new StringBuilder();
    builder.append("Options: ");
    builder.append("\n");
    int i = 0;
    for (ConfigOption option : ConfigOption.values()) {
      i++;
      builder.append(option.option);
      builder.append(getPadding(option));
      builder.append(indent(option.help));
      builder.append("\n");
      if (i%3 == 0) {
        builder.append("\n");
      }
    }
    System.out.println(builder.toString());
  }

  private static void executeConfigOperation(ConfigOption mapr) throws IOException {
    assert mapr.name().startsWith("MAPR");
    assert mapr.other.name().startsWith("APACHE");

    Map<String, String> maprConfMap = getConfigurationAsMap(getConfigurationFor(mapr));
    Map<String, String> apacheConfMap = getConfigurationAsMap(getConfigurationFor(mapr.other));

    Configuration result = new Configuration(false);
    for (String key : maprConfMap.keySet()) {
      String apacheValue = apacheConfMap.get(key);
      String maprValue = maprConfMap.get(key);
      switch(mapr.operation) {
        case FIND_MAPR_MODIFIED_PROPERTIES: {
          if (apacheValue != null && !apacheValue.equals(maprValue)) {
            result.set(key, maprValue);
          } else if (apacheValue == null && maprValue != null) {
            result.set(key, maprValue);
          }
        }
        break;

        case FIND_MAPR_ADDED_PROPERTIES: {
          if (apacheValue == null || apacheValue.isEmpty()) {
            result.set(key, maprValue);
          }
        }
        break;
      }
    }
    result.writeXml(System.out);
    System.out.println();
  }

  private static Map<String, String> getConfigurationAsMap(Configuration conf) {
    Map<String, String> confMap = new HashMap<>();
    Iterator<Entry<String,String>> iterator = conf.iterator();
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      confMap.put(entry.getKey(), entry.getValue());
    }
    return confMap;
  }


  private static void printPropertiesForOption(ConfigOption option) throws IOException {
    Configuration configuration = getConfigurationFor(option);
    configuration.writeXml(System.out);
    System.out.println();
  }

  private static Configuration getConfigurationFor(ConfigOption option) {
    Configuration configuration = new Configuration(false);

    for (String source : option.sources) {
      configuration.addResource(source);
    }
    return configuration;
  }

  public static void main(String[] args) throws IOException {
    // Configuration seems to throw NPE if deprecated keys are not added. Adding it here so that it's common
    // for all the various options.
    ConfigUtil.addDeprecatedKeys();

    if (args.length == 0) {
      printHelp();
    } else if (args.length > 0 && OPTIONS.containsKey(args[0])) {
      ConfigOption option = OPTIONS.get(args[0]);
      if (option != null && option.other != null && option.operation != null) {
        executeConfigOperation(option);
      } else if (option != null) {
        printPropertiesForOption(option);
      } else {
        printHelp();
      }
    } else {
      printHelp();
    }
  }

}
