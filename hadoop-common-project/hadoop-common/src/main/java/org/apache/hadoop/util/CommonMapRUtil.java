package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CommonMapRUtil {

  private static final String MAPR_CLUSTER_FILE_NAME = "/conf/mapr-clusters.conf";
  private volatile String defaultClusterName = "default";
  private boolean isSecurityEnabled;
  private final String JNISecurity = "com.mapr.security.JNISecurity";
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
    initCurrentClusterName();
    initSecurityStatus();
  }


  public synchronized void initCurrentClusterName() {
    String maprHome = BaseMapRUtil.getPathToMaprHome();
    String clusterConfFile = maprHome + MAPR_CLUSTER_FILE_NAME;
    String tempClusterName = null;
    try {
      String strLine;
      String clusterName = defaultClusterName;
      BufferedReader bfr = new BufferedReader(new FileReader(clusterConfFile));
      while ((strLine = bfr.readLine()) != null) {
        if (strLine.matches("^\\s*#.*")) {
          String[] tokens = strLine.split("[\\s]+");
          if (tokens.length < 2) continue;
          clusterName = tokens[0];
        }
        if (tempClusterName != null) continue;
        this.defaultClusterName = clusterName;
        tempClusterName = clusterName;
      }
    } catch (IOException bfr) {
      LOG.info("Cannot read cluster name");
      bfr.printStackTrace();
    }
  }

  public void initSecurityStatus() {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(JNISecurity);
      Method getSecurityStatus = klass.getDeclaredMethod("IsSecurityEnabled", String.class);
      isSecurityEnabled = (boolean) getSecurityStatus.invoke(null, getCurrentClusterName());
    } catch (ClassNotFoundException err) {
      LOG.info("Cannot find JNISecurity class at classpath");
      err.printStackTrace();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException err) {
      LOG.info("Cannot execute IsSecurityEnabled method");
      err.printStackTrace();
    }
  }

  public String getCurrentClusterName() {
    return this.defaultClusterName;
  }

  public boolean isSecurityEnabled() {
    return this.isSecurityEnabled;
  }
}
