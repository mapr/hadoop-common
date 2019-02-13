package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class CommonMapRUtil {

  private static final String MAPR_CLUSTER_FILE_NAME = "/conf/mapr-clusters.conf";
  private volatile String defaultClusterName = "default";
  private boolean securityEnabled = false;
  private boolean kerberosEnabled = false;
  private static CommonMapRUtil s_instance;

  private static final Log LOG = LogFactory.getLog(CommonMapRUtil.class);

  private CommonMapRUtil() {
    this.init();
  }

  public static CommonMapRUtil getInstance() {
    return s_instance;
  }

  static {
    s_instance = new CommonMapRUtil();
  }

  public synchronized void init() {
    String maprHome = BaseMapRUtil.getPathToMaprHome();
    String clusterConfFile = maprHome + MAPR_CLUSTER_FILE_NAME;
    String tempClusterName = null;
    try {
      String strLine;
      BufferedReader bfr = new BufferedReader(new FileReader(clusterConfFile));
      while ((strLine = bfr.readLine()) != null) {
        String[] tokens;
        if (strLine.matches("^\\s*#.*") || (tokens = strLine.split("[\\s]+")).length < 2) continue;
        Object clusterName = tokens[0];
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
        if (tempClusterName != null) continue;
        this.defaultClusterName = (String) clusterName;
        tempClusterName = (String) clusterName;
      }
    }
    catch (FileNotFoundException bfr) {
    }
    catch (Throwable t) {
      LOG.error("Exception during init", t);
    }
  }

  public boolean isSecurityEnabled(){
    return securityEnabled;
  }

  public boolean isKerberosEnabled(){
    return kerberosEnabled;
  }

  private int setClusterOption(String property, String arg){
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


}
