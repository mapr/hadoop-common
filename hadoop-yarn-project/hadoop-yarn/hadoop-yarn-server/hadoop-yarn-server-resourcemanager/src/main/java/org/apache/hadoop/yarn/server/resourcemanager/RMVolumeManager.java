/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.util.YarnAppUtil;
import org.apache.hadoop.util.RMVolumeShardingUtil;
import org.apache.hadoop.yarn.server.volume.VolumeManager;

/**
 * Manage resource manager volume and directory creation on MapRFS.
 */
public class RMVolumeManager extends VolumeManager {

  private int volumeCount;
  private boolean useVolumeSharding;
  private String rmDir;
  private String rmSystemDir;
  private String rmStagingDir;

  public RMVolumeManager() {
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
    super.serviceInit(conf);
    LOG = LoggerFactory.getLogger(RMVolumeManager.class);
    volumeMode = "yarn";
    volumeLogfilePath = volumeLogfilePath + "/logs/createRMVolume.log";
    rmDir = conf.get(YarnDefaultProperties.RM_DIR, YarnDefaultProperties.DEFAULT_RM_DIR);
    mountPath = rmDir;
    rmSystemDir = conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR);
    rmStagingDir = conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR);
    volumeCount = conf.getInt(YarnDefaultProperties.RM_DIR_VOLUME_COUNT, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_COUNT);
    useVolumeSharding = conf.getBoolean(YarnDefaultProperties.RM_DIR_VOLUME_SHARDING_ENABLED, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_SHARDING_ENABLED)
            && new Path(rmStagingDir).toUri().getRawPath().startsWith(new Path(rmDir).toUri().getRawPath())
            && new Path(rmSystemDir).toUri().getRawPath().startsWith(new Path(rmDir).toUri().getRawPath());

    createVolumes(conf);

    RMVolumeShardingUtil.rebalanceVolumes(rmSystemDir, volumeCount, useVolumeSharding, rmDir, fs);
    RMVolumeShardingUtil.rebalanceVolumes(rmStagingDir, volumeCount, useVolumeSharding, rmDir, fs);
  }

  @Override
  public void createVolumes(Configuration conf) throws Exception {
    waitForYarnPathCreated(conf);

      // create separate volume for general RM dir
    createVolume("");
    createDir(conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR),
            YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

    createDir(conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR),
            YarnAppUtil.RM_STAGING_DIR_PERMISSION);

    if(useVolumeSharding) {
      for (int volumeNumber = 0; volumeNumber < volumeCount; volumeNumber++) {
        createVolume(Integer.toString(volumeNumber));
        createDir(rmSystemDir.replaceAll(rmDir, rmDir + Path.SEPARATOR + volumeNumber),
                YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

        createDir(rmStagingDir.replaceAll(rmDir, rmDir + Path.SEPARATOR + volumeNumber),
                YarnAppUtil.RM_STAGING_DIR_PERMISSION);
      }
    }
  }
}
