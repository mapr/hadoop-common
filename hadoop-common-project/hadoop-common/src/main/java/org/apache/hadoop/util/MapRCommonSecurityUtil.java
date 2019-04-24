package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class MapRCommonSecurityUtil {

  public static final String DEFAULT_INSTALL_LOCATION = "/opt/mapr";
  private static final String CLUSTER_CONFIG_LOCATION = "/conf/mapr-clusters.conf";
  private String currentClusterName;
  private static MapRCommonSecurityUtil s_instance;
  private boolean isClusterSecure = false;
  private boolean isClusterValid = false;
  private Set<String> clustersNamesList = new HashSet<>();


  private static final Log LOG = LogFactory.getLog(MapRCommonSecurityUtil.class);

  private MapRCommonSecurityUtil() {
    this.init();
  }

  public static MapRCommonSecurityUtil getInstance() {
    return s_instance;
  }

  static {
    s_instance = new MapRCommonSecurityUtil();
  }

  public void init() {
    parseMaprClustersConf();
  }

  /**
   * Obtains the full path name of the file containing the user ticket. This is determined as follows:
   * <ul>
   *   <li>If the environment variable MAPR_TICKETFILE_LOCATION is set, then return the value of this
   *   environment variable.</li>
   *   <li>Otherwise, the default location of the user ticket is as follows:
   *     <ul>
   *       <li>For Windows, this is at %TEMP%\maprticket_&lt;username&gt;. Example: C:\Temp\maprticket_joe.</li>
   *       <li>For all other operating systems, this is at /tmp/maprticket_&lt;uid&gt; where &lt;uid&gt; is the effective UID of calling process.</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @return The full path name of the file containing the user ticket.
   * @throws MapRCommonSecurityException Thrown when the location of the user ticket cannot be determined
   */
  public String getUserTicketAndKeyFileLocation() throws MapRCommonSecurityException {
    String mapRDefaultKeyFileLocation_;
    String mapRFileNameSuffix_;
    String filePath;
    String euid_;

    filePath= System.getenv("MAPR_TICKETFILE_LOCATION");
    if (filePath != null) {
      if (!filePath.isEmpty()) {
        return filePath;
      }
    }
    String osName=System.getProperty("os.name");
    if (osName.equalsIgnoreCase("Windows")) {
      mapRDefaultKeyFileLocation_ = System.getenv("TEMP");
      mapRFileNameSuffix_ = System.getProperty("user.name");
    } else {
      mapRDefaultKeyFileLocation_ = "/tmp";
      /*
       * The default file name is /tmp/maprticket_<uid>. For example, if this ticket is for
       * user mapr and the UID of mapr is 5000, then the default file name is /tmp/maprticket_<uid>
       */
      String userName = System.getProperty ("user.name");
      ArrayList<String> command = new ArrayList<String>();
      command.add ("id");
      command.add ("-u");
      command.add (userName);
      try {
        euid_ = executeCommandAndReturnOutput(command);
      } catch (IOException e) {
        LOG.error("Unable to obtain effective UID for user " + userName + ":" + e.getMessage());
        throw new MapRCommonSecurityException ("Unable to obtain effective UID for user " + userName + ":" + e.getMessage());
      } catch (InterruptedException e) {
        LOG.error("Error executing command id -u " + userName + ": " + e.getMessage());
        throw new MapRCommonSecurityException ("Error execuring command id -u " + userName + ": " + e.getMessage());
      }
      mapRFileNameSuffix_ = euid_;
    }
    filePath = mapRDefaultKeyFileLocation_ + File.separator + "maprticket_" + mapRFileNameSuffix_;

    return filePath;
  }

  /*
   * Parses the mapr-clusters.conf file to obtain the current cluster name and the
   * cluster security status. Invoked by the constructor
   */
  private void parseMaprClustersConf() {
    String clusterConfig;
    String installDir;
    String maprHomeDir= System.getenv("MAPR_HOME");
    if (maprHomeDir != null) {
      if (!maprHomeDir.isEmpty()) {
        installDir = maprHomeDir;
      } else {
        installDir = DEFAULT_INSTALL_LOCATION;
      }
    } else
      installDir = DEFAULT_INSTALL_LOCATION;

    clusterConfig = installDir + CLUSTER_CONFIG_LOCATION;

    try {
      File file = new File(clusterConfig);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      /*
       * Each line of mapr-cluster.conf has a format like this:
       * chyelin61.cluster.com secure=true node-61.lab:7222
       */
      boolean firstLine = true;
      String thisCluster;
      while ((line = bufferedReader.readLine()) != null) {
        /*
         * At this point, we have a line containing the cluster name and its
         * security status. Split this into space-delimited tokens and look
         * for the one with the secure=flag
         */
        String[] elements = line.split (" ");
        /*
         * This is the cluster name for this entry
         */
        thisCluster = elements[0];
        if (firstLine) {
          if (currentClusterName == null) {
            currentClusterName = thisCluster;
          }
          firstLine = false;
        }

        clustersNamesList.add(thisCluster);
        /*
         * We need to find an entry with a matching cluster name
         */
        if (!currentClusterName.equals(thisCluster))
          continue;
        /*
         * If we get here, we have a matching cluster entry
         */
        isClusterValid = true;
        /*
         * See if the cluster is secure
         */
        for (int i = 1; i<elements.length; i++) {
          if (elements[i].startsWith("secure=")) {
            String[] secureSetting = elements[i].split ("=");
            isClusterSecure=false;
            if (secureSetting[1].equalsIgnoreCase("true")) {
              isClusterSecure=true;
            }
            break;
          }
        }
      }
      bufferedReader.close();
    } catch (IOException e) {
      LOG.error ("Failed to parse mapr-clusters.conf: " + e.getMessage());
    }
  }

  /*
   * Executes the command given in the array "command" and returns the output as a string
   * Used mainly as an equivalent to C system calls
   */
  private String executeCommandAndReturnOutput(ArrayList<String> command) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();
    StringBuilder processOutput = new StringBuilder();

    try (
        BufferedReader processOutputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));)
    {
      String readLine;
      while ((readLine = processOutputReader.readLine()) != null)
      {
        processOutput.append(readLine + System.lineSeparator());
      }
      process.waitFor();
    }
    return processOutput.toString().trim();
  }

  public boolean isSecurityEnabled() {
    return isClusterSecure;
  }

  public String getClusterName() {
    return currentClusterName;
  }

  public Set<String> getClustersNamesList(){return  clustersNamesList; }

}
