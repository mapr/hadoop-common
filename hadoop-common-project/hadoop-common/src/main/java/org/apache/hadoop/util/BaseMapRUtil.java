package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class BaseMapRUtil {

  private static final String MAPR_ENV_VAR = "MAPR_HOME";
  private static final String MAPR_PROPERTY_HOME = "mapr.home.dir";
  private static final String MAPR_HOME_PATH_DEFAULT = "/opt/mapr";
  public static final String HOST_NAME_FILE_PATH = getPathToMaprHome() + "/hostname";
  private static String hostname = null;

  private static final Log LOG = LogFactory.getLog(BaseMapRUtil.class);

  public static String getPathToMaprHome() {
    String maprHome = System.getenv(MAPR_ENV_VAR);
    if (maprHome == null) {
      maprHome = System.getProperty(MAPR_PROPERTY_HOME);
      if (maprHome == null) {
        return MAPR_HOME_PATH_DEFAULT;
      }
    }
    return maprHome;
  }

  public static synchronized String getMapRHostName() {
    if (hostname == null) {
      hostname = readMapRHostNameFromFile(HOST_NAME_FILE_PATH);
    }
    return hostname;
  }

  private static String readMapRHostNameFromFile(String filePath) {
    if (! new File(filePath).exists()) {
      LOG.debug(filePath + " does not exist. Assuming client-only installation..");
      return "MAPR_CLIENT";
    }

    FileReader freader = null;
    BufferedReader breader = null;
    try {
      freader = new FileReader(filePath);
      breader = new BufferedReader(freader);
      return breader.readLine();
    } catch (Exception e) {
      LOG.warn("Error while reading " + filePath, e);
    } finally {
      try {
        if (breader != null) {
          breader.close();
        }
      } catch (IOException t) {
        LOG.error("Failed to close buffered reader", t);
      }
      try {
        if (freader != null) {
          freader.close();
        }
      } catch (IOException t) {
        LOG.error("Failed to close " + filePath, t);
      }
    }
    return null;
  }

  public static synchronized void setMapRHostName(String host) {
    if (hostname == null)
      hostname = host;
  }

}
