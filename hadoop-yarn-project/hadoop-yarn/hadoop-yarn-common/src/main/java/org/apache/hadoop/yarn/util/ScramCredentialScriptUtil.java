package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.MaprShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ScramCredentialScriptUtil {

  private static final String HADOOP_HOME_PROPERTY = "hadoop.home.dir";
  private static final String YARN_HOME_PROPERTY = "yarn.home.dir";
  private static final String scramScript = "scramConfigure.sh";

  public static void checkAndCopyScramCreds(Configuration conf, String serviceName) {
    MaprShellCommandExecutor test = new MaprShellCommandExecutor();
    Map params = new HashMap();
    params.put("service", serviceName);
    params.put("path", conf.get(YarnDefaultProperties.YARN_DIR, YarnDefaultProperties.DEFAULT_YARN_DIR));
    try {
      test.execute(new String[]{getPathToHadoopHome() + "/bin/" + scramScript}, params, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String getPathToHadoopHome() {
    String hadoopHome = System.getProperty(HADOOP_HOME_PROPERTY);
    if (hadoopHome == null) {
      hadoopHome = System.getProperty(YARN_HOME_PROPERTY);
      if (hadoopHome == null) {
        return null;
      }
    }
    return hadoopHome;
  }
}
