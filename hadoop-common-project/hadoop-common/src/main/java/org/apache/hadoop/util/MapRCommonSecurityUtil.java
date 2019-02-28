package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class MapRCommonSecurityUtil {

  private static final String MAPR_CLUSTER_FILE_NAME = "/conf/mapr-clusters.conf";
  private volatile String defaultClusterName = "default";
  private static MapRCommonSecurityUtil s_instance;
  private boolean securityEnabled = false;
  private boolean kerberosEnabled = false;


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

  public synchronized void init() {
    readMapRClusterConf();
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

  private void readMapRClusterConf() {
    String maprHome = BaseMapRUtil.getPathToMaprHome();
    String clusterConfFile = maprHome + MAPR_CLUSTER_FILE_NAME;
    try {
      String strLine;
      BufferedReader bfr = new BufferedReader(new FileReader(clusterConfFile));
      while ((strLine = bfr.readLine()) != null) {
        String[] tokens;
        if (strLine.matches("^\\s*#.*") || (tokens = strLine.split("[\\s]+")).length < 3) continue;
        String clusterName = tokens[0];
        for (int i = 1; i < tokens.length; ++i) {
          if (tokens[i].contains("=")) {
            String[] arr = tokens[i].split("=");
            if (arr.length == 2){
              if (setClusterOption(arr[0], arr[1]) == 0) continue;
            }
            LOG.error("Can't parse options:" + tokens[i] + " for cluster " + clusterName);
            continue;
          }
        }
        this.defaultClusterName = clusterName;
        break;
      }
    } catch (IOException bfr) {
      LOG.error("Exception during read cluster name", bfr);
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

  private int setClusterOption(String property, String arg) {
    switch (property) {
      case "secure":
        securityEnabled = Boolean.parseBoolean(arg);
        return 0;
      case "kerberosEnable":
        kerberosEnabled = Boolean.parseBoolean(arg);
        return 0;
      default:
        return 1;
    }
  }

  public boolean isSecurityEnabled() {
    return securityEnabled;
  }

  public String getClusterName() {
    return defaultClusterName;
  }

}
