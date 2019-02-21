package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

public class CommonMapRUtil {

  private static final String MAPR_CLUSTER_FILE_NAME = "/conf/mapr-clusters.conf";
  private volatile String defaultClusterName = "default";
  private static CommonMapRUtil s_instance;
  private static String JNISecurity = "com.mapr.security.JNISecurity";
  private static String CLDBRpcCommonUtils = "com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils";
  private Method getUserTicketMethod = null;
  private Method securityEnabled = null;
  private boolean isSecurityEnabled = false;


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
    loadJNISecurityMethods();
    readClusterName();
    checkSecurity();
  }

  private void readClusterName() {
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
        if (tempClusterName != null) continue;
        this.defaultClusterName = (String) clusterName;
        tempClusterName = (String) clusterName;
      }
    } catch (IOException bfr) {
      LOG.error("Exception during read cluster name", bfr);
    }
  }

  private void loadJNISecurityMethods() {
    String nativeLibraryName = System.mapLibraryName("MapRClient");
    System.load(findFileOnClassPath(nativeLibraryName));
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(JNISecurity);
      getUserTicketMethod = klass.getDeclaredMethod("GetUserTicketAndKeyFileLocation");
      securityEnabled = klass.getDeclaredMethod("IsSecurityEnabled", String.class);
    } catch (ClassNotFoundException err) {
      LOG.info("Cannot find JNISecurity class at classpath");
      err.printStackTrace();
    } catch (NoSuchMethodException err) {
      LOG.info("Cannot find method");
      err.printStackTrace();
    }
  }

  private String findFileOnClassPath(String fileName) {
    String classpath = System.getProperty("java.class.path");
    StringTokenizer token = new StringTokenizer(classpath, System.getProperty("path.separator"));
    while (token.hasMoreTokens()) {
      String pathElement = token.nextToken();
      File path = new File(pathElement);
      File absolutePath = new File(pathElement).getAbsoluteFile();
      if (path.isFile()) {
        File target = new File(path.getParent(), fileName);
        if (target.exists()) {
          return target.getAbsolutePath();
        }
      } else {
        File target = new File(absolutePath, fileName);
        if (target.exists()) {
          return target.getAbsolutePath();
        }
      }
    }
    return null;
  }

  public void checkSecurity() {
    try {
      //call JNISecurity.SetParsingDone();
      Class<?> cldbClass = Thread.currentThread().getContextClassLoader().loadClass(CLDBRpcCommonUtils);
      cldbClass.getDeclaredMethod("getInstance").invoke(null);
      isSecurityEnabled = (boolean) securityEnabled.invoke(null, defaultClusterName);
    } catch (ClassNotFoundException e) {
      LOG.info("Cannot find CLDBRpcCommonUtils class at classpath");
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      LOG.info("Cannot find method");
      e.printStackTrace();
    } catch (IllegalAccessException | InvocationTargetException err) {
      LOG.info("Cannot execute isSecurityEnabled method");
      err.printStackTrace();
    }
  }

  public Method getGetUserTicketMethod() {
    return getUserTicketMethod;
  }

  public boolean isSecurityEnabled() {
    return isSecurityEnabled;
  }

  public String getClusterName() {
    return defaultClusterName;
  }

}
