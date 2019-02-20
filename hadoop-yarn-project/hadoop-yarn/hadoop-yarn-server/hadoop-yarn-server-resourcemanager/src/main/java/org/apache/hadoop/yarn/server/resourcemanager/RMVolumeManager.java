/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.server.api.ConfigurableAuxiliaryService;

import org.apache.hadoop.util.BaseMapRUtil;
import org.apache.hadoop.yarn.util.YarnAppUtil;

/**
 * Manage resource manager volume and directory creation on MapRFS.
 */
public class RMVolumeManager extends ConfigurableAuxiliaryService {
  private static final Log LOG = LogFactory.getLog(RMVolumeManager.class);

  private static final String RM_VOLUME_SCRIPT_PATH = "/server/createJTVolume.sh";
  private static final String RM_VOLUME_LOGFILE_PATH = "/logs/createRMVolume.log";

  RMVolumeManager() {
    super(YarnDefaultProperties.RM_VOLUME_MANAGER_SERVICE);
  }

  /**
   * The volume creation is done as part of serviceInit instead of serviceStart
   * because in the case of JobHistoryServer, there are services which try to
   * create directories inside this volume in serviceInit. Since serviceInit is
   * called sequentially for all services before serviceStart, we need this
   * behavior.
   */
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    createVolume(conf);

    FileSystem fs = FileSystem.get(conf);
    createDir(fs, conf.get(YarnDefaultProperties.RM_SYSTEM_DIR),
        YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

    createDir(fs, conf.get(YarnDefaultProperties.RM_STAGING_DIR),
        YarnAppUtil.RM_STAGING_DIR_PERMISSION);
  }

  private void createVolume(Configuration conf) throws Exception {
    String maprInstallDir = BaseMapRUtil.getPathToMaprHome();

    String clusterPath = conf.get("cluster.name.prefix");
    String mountPath = conf.get(YarnDefaultProperties.RM_DIR);
    if ( clusterPath != null ) {
      // script considers only one level of directory after volume mount point
      // so it can substruct one level and get to the mount point
      // we can not change script behavior as it is only used by JT
      // that is why doing this "hack" 
      mountPath = mountPath.substring(0, mountPath.length() - "/rm".length());
    }
    
    String[] args = new String[5];
    args[0] = maprInstallDir + RM_VOLUME_SCRIPT_PATH;
    args[1] = BaseMapRUtil.getMapRHostName();
    args[2] = mountPath;
    args[3] = conf.get(YarnDefaultProperties.RM_DIR);
    args[4] = "yarn";  // To distinguish RM from JT

    // Set MAPR_MAPREDUCE_MODE to "yarn" since this is in ResourceManager and
    // hadoop commands invoked should be with the hadoop2 script
    Map<String, String> env = new HashMap<String, String>();
    env.put("MAPR_MAPREDUCE_MODE", "yarn");

    ShellCommandExecutor shexec = new ShellCommandExecutor(args, null, env);
    if (LOG.isInfoEnabled())
      LOG.info("Checking for ResourceManager volume." +
               " If volume not present command will create and mount it." +
               " Command invoked is : " + shexec.toString());

    // Since the same volume creation could happen simultaneously
    // (RM and History server), it is possible to get an exception.
    // Both the calls could end up trying to create the volume and so
    // one of them will fail. When the failed caller tries again, it will
    // see the volume to be already created and so would be a NOOP.
    int numAttempts = 3;
    for (int i = 0; i < numAttempts; i++) {
      try {
        shexec.execute();

        // Success
        break;
      } catch (IOException ioe) {
        // Propage the exception if this is the last attempt
        if (i == numAttempts - 1) {
          int exitCode = shexec.getExitCode();
          if (exitCode != 0) {
            LOG.error("Failed to create and mount ResourceManager volume at "
                + args[2] + ". Please see logs at " + maprInstallDir
                + RM_VOLUME_LOGFILE_PATH);

            LOG.error("Command ran " + shexec.toString());
            LOG.error("Command output " + shexec.getOutput());
          }
          throw ioe;
        } else {
          // Wait and retry
          Thread.sleep(100);
          if (LOG.isInfoEnabled()) {
            LOG.info("Retrying check for ResourceManager volume ... ");
          }
        }
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Sucessfully created ResourceManager volume and mounted at "
               + args[2]);
    }
  }

  private void createDir(FileSystem fs, String pathName,
      FsPermission perm) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Creating RM dir: " + pathName + " with permission: " + perm);
    }
    FileSystem.mkdirs(fs, new Path(pathName), perm);
  }
}
