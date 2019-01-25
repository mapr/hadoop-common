package org.apache.hadoop.yarn.configuration;

  import java.io.BufferedReader;
  import java.io.File;
  import java.io.FileInputStream;
  import java.io.IOException;
  import java.io.InputStream;
  import java.io.InputStreamReader;

public class YarnSiteAclXmlBuilder {


  private static final String CONFIG_DEMARCATION = "  <!-- :::CAUTION::: DO NOT EDIT ANYTHING ON OR ABOVE THIS LINE -->";

  // template for yarn-site.xml
  private static final String YARN_SITE_TEMPLATE_FILE = "yarn-site-acl-template.xml";

  private final String pathToYarnSiteXml;

  public YarnSiteAclXmlBuilder(String pathToYarnSiteXml) {
    this.pathToYarnSiteXml = pathToYarnSiteXml;
  }

  /**
   * Builds the yarn-site.xml file, either freshly from
   * {@link #YARN_SITE_TEMPLATE_FILE} or by merging with {@link #pathToYarnSiteXml}.
   *
   * If {@link #pathToYarnSiteXml} is a valid file, then the contents of the file are
   * read and {@link #CONFIG_DEMARCATION} is replaced with
   * the new contents returned by {@link #buildYarnSiteXmlFromTemplate()}.
   *
   * @return
   * @throws IOException
   */
  public String build() throws IOException {
    String currentContents = new File(pathToYarnSiteXml).exists() ?
      readStream(new FileInputStream(pathToYarnSiteXml)) : "";
    if (currentContents.contains(CONFIG_DEMARCATION)) {
      return currentContents.replaceAll(CONFIG_DEMARCATION + "\r?\n",
        buildYarnSiteXmlFromTemplate().replaceAll("(?s)</configuration>.*", ""));
    } else {
      return buildYarnSiteXmlFromTemplate();
    }
  }

  /**
   * Builds the yarn-site.xml from {@link #YARN_SITE_TEMPLATE_FILE}.
   *
   * @return
   * @throws IOException
   */
  private String buildYarnSiteXmlFromTemplate() throws IOException {
    InputStream aclTemplateStream = Thread.currentThread()
      .getContextClassLoader()
      .getResourceAsStream(YARN_SITE_TEMPLATE_FILE);

    return readStream(aclTemplateStream);
  }

  private String readStream(InputStream stream) throws IOException {
    StringBuilder buf = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    String line;
    while ((line = reader.readLine()) != null) {
      buf.append(line);
      buf.append("\n");
    }
    reader.close();
    return buf.toString();
  }

  /**
   * Usage : YarnSiteAclXmlBuilder pathToYarnSiteXml
   */
  public static void main(String args[]) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: YarnSiteAclXmlBuilder <Full path To yarn-site.xml>");
      System.exit(1);
    }

    String newYarnSiteXml = new YarnSiteAclXmlBuilder(args[0]).build();
    System.out.println(newYarnSiteXml);
  }
}

