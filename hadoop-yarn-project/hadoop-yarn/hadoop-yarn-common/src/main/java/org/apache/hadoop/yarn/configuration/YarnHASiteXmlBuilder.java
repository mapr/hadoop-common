/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.configuration;

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class YarnHASiteXmlBuilder {

  private static final String RM_ID_TEMPLATE = "__RM_ID__";
  private static final String IP_FOR_RM_ID_TEMPLATE = "__IP_FOR_RM_ID__";

  private static final String THIS_RM_ID_TEMPLATE = "__THIS_RM_ID__";
  private static final String RM_IDS_TEMPLATE = "__COMMA_SEPARATED_RM_IDS__";
  private static final String MAPR_CLUSTER_ID_TEMPLATE = "__MAPR_CLUSTER_ID__";
  private static final String ZK_IP_PORT_TEMPLATE = "__ZK_IP_PORT__";
  private static final String RM_CONFIGS_TEMPLATE = "__THIS_LINE_WILL_BE_REPLACED_BY_CONFIGS_FOR_INDIVIDUAL_RMS__";
  private static final String CONFIG_DEMARCATION = "  <!-- :::CAUTION::: DO NOT EDIT ANYTHING ON OR ABOVE THIS LINE -->";

  // template for yarn-site.xml. Contains the various params to enable RM HA.
  private static final String YARN_SITE_HA_TEMPLATE_FILE = "yarn-site-ha-template.xml";

  // template for each RM in a HA configuration. Each RM needs to get a unique ID.
  private static final String YARN_HA_RM_ID_TEMPLATE_FILE = "yarn-ha-rm-id-configuration.template";

  private final String[] rmIps;
  private final String maprClusterName;
  private final String zkIpPort;
  private final String pathToYarnSiteXml;

  private String rmIdConfigTemplate;

  public YarnHASiteXmlBuilder(String[] rmIps, String clusterName, String zkIpPort, String pathToYarnSiteXml) {
    this.rmIps = rmIps;
    this.maprClusterName = clusterName;
    this.zkIpPort = zkIpPort;
    this.pathToYarnSiteXml = pathToYarnSiteXml;
  }

  /**
   * Builds the yarn-site.xml file, either freshly from
   * {@link #YARN_SITE_HA_TEMPLATE_FILE} or by merging with {@link #pathToYarnSiteXml}.
   *
   * If {@link #pathToYarnSiteXml} is a valid file, then the contents of the file are
   * read and anything before and including {@link #CONFIG_DEMARCATION} is replaced with
   * the new contents returned by {@link #buildYarnSiteXmlFromTemplate()}.
   *
   * @return
   * @throws IOException
   */
  public String build() throws IOException{
    String currentContents = new File(pathToYarnSiteXml).exists() ?
        readStream(new FileInputStream(pathToYarnSiteXml)) : "";
    if (currentContents.contains(CONFIG_DEMARCATION)) {
      return currentContents.replaceAll("(?s).*" + CONFIG_DEMARCATION + "\r?\n",
          buildYarnSiteXmlFromTemplate().replaceAll("(?s)</configuration>.*", ""));
    } else {
      return buildYarnSiteXmlFromTemplate();
    }
  }

  /**
   * Builds the yarn-site.xml from {@link #YARN_SITE_HA_TEMPLATE_FILE}.
   * On RM nodes, the yarn.resourcemanager.ha.id is set to rm{1,2,3,..}.
   * On non-RM nodes, yarn.resourcemanager.ha.id is set to rm1.
   *
   * @return
   * @throws IOException
   */
  private String buildYarnSiteXmlFromTemplate() throws IOException {
    String[] rmIds = new String[rmIps.length];
    StringBuilder rmConfigs = new StringBuilder();
    for (int i = 0; i < rmIps.length; i++) {
      rmConfigs.append(getRMConfigForIp(rmIps[i]));
      rmIds[i] = getRmIdForIp(rmIps[i]);
    }
    InputStream haTemplateStream = Thread.currentThread()
        .getContextClassLoader()
        .getResourceAsStream(YARN_SITE_HA_TEMPLATE_FILE);

    return readStream(haTemplateStream)
        .replace(RM_IDS_TEMPLATE, Joiner.on(",").join(rmIds))
        .replace(MAPR_CLUSTER_ID_TEMPLATE, maprClusterName)
        .replace(ZK_IP_PORT_TEMPLATE, zkIpPort)
        .replace(THIS_RM_ID_TEMPLATE, getRmIdForIp(getThisRmIp()))
        .replaceAll("(?m)^.*" + RM_CONFIGS_TEMPLATE + ".*$", rmConfigs.toString());
  }

  private String getRMConfigForIp(String rmIp) throws IOException {
    return getRmIdConfigTemplate()
        .replace(RM_ID_TEMPLATE, getRmIdForIp(rmIp))
        .replace(IP_FOR_RM_ID_TEMPLATE, rmIp);
  }

  private String getRmIdConfigTemplate() throws IOException {
    if (rmIdConfigTemplate == null) {
      rmIdConfigTemplate = readStream(Thread.currentThread()
          .getContextClassLoader()
          .getResourceAsStream(YARN_HA_RM_ID_TEMPLATE_FILE));
    }
    return rmIdConfigTemplate;
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

  private String getRmIdForIp(String ip) {
    for (int i = 0; i < rmIps.length; i++) {
      if (rmIps[i].equals(ip)) {
        return "rm" + String.valueOf(i + 1);
      }
    }
    return "rm1";
  }

  /**
   * Looks up the current host's ip/hostname in the {@link #rmIps} list.
   *
   * @return
   */
  private String getThisRmIp() {
    String thisRmIp = null;
    try {
      List<InetAddress> allInetAddressesOfThisMachine = getAllInetAddressesOfThisMachine();
      for (String rmIp : rmIps) {
        for (InetAddress rmAddr : InetAddress.getAllByName(rmIp)) {
          if (doesAddrBelongToThisHost(rmAddr, allInetAddressesOfThisMachine)) {
            if (thisRmIp == null) {
              thisRmIp = rmIp;
              break;
            } else {
              throw new RuntimeException(thisRmIp + " and " + rmIp + " seem to resolve to same machine. " +
                  "Please input distinct addresses for Resource Manager hosts.");
            }
          }
        }
      }
      return thisRmIp;
    } catch (SocketException e) {
      throw new RuntimeException("Error in looking up the network interfaces of local host.", e);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Error in looking up the address for local host.", e);
    }
  }

  private boolean doesAddrBelongToThisHost(InetAddress addr, List<InetAddress> thisHostsAddresses) {
    for (InetAddress hostAddr : thisHostsAddresses) {
      if (addr.equals(hostAddr)) {
        return true;
      }
    }
    return false;
  }


  private List<InetAddress> getAllInetAddressesOfThisMachine() throws SocketException, UnknownHostException {
    List<InetAddress> allIps = new ArrayList<>();
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    for (NetworkInterface networkInterface : Collections.list(interfaces))  {
      allIps.addAll(Collections.list(networkInterface.getInetAddresses()));
    }
    allIps.add(InetAddress.getLocalHost());
    return allIps;
  }

  /**
   * Usage : YarnHASiteXmlBuilder rmIPs clusterName zkIpPort pathToYarnSiteXml
   */
  public static void main(String args[]) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: YarnHASiteXmlBuilder <Comma separated RM IPs> " +
          "<MapR cluster name> <ZK IP:Port (optional)> <Full path To yarn-site.xml>");
      System.exit(1);
    }
    String[] rmIPs = args[0].split(",");
    if (rmIPs.length < 2) {
      System.err.println("At least 2 RM IPs are required to configure RM HA.");
      System.exit(1);
    }

    // for client-only installations, ZK address is not provided to configure.sh
    String zkAddr = args.length > 3 ? args[2] : " ";
    String yarnSiteXmlPath = args.length > 3 ? args[3] : args[2];
    System.out.println(new YarnHASiteXmlBuilder(rmIPs, args[1], zkAddr, yarnSiteXmlPath).build());
  }

}
