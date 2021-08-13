package org.apache.hadoop.yarn.server.applicationhistoryservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.util.YarnAppUtil;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.RMVolumeShardingUtil;
import org.apache.hadoop.yarn.server.volume.VolumeManager;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HSVolumeManager extends VolumeManager {

  public HSVolumeManager() {
    super(YarnDefaultProperties.APP_HISTORY_VOLUME_MANAGER_SERVICE);
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
    LOG = LoggerFactory.getLogger(HSVolumeManager.class);

    if(RMVolumeShardingUtil.isVolumeScriptNewVersion()) {
      mountPath = conf.get(YarnDefaultProperties.APP_HISTORY_STAGING_DIR, YarnDefaultProperties.DEFAULT_APP_HISTORY_STAGING_DIR);
      volumeMode = "hs";
      volumeLogfilePath = volumeLogfilePath + "/logs/createJHSVolume.log";
      createVolumes(conf);
      fs.setPermission(new Path(mountPath), YarnAppUtil.RM_STAGING_DIR_PERMISSION);
      this.moveHistoryDataToNewVolume(conf);
    } else {
      mountPath = conf.get(YarnDefaultProperties.RM_DIR, YarnDefaultProperties.DEFAULT_RM_DIR);
      volumeMode = "yarn";
      volumeLogfilePath = volumeLogfilePath + "/logs/createRMVolume.log";
      createVolumes(conf);
      createDir(conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR),
              YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

      createDir(conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR),
              YarnAppUtil.RM_STAGING_DIR_PERMISSION);
    }
  }

  @Override
  public void createVolumes(Configuration conf) throws Exception {
    waitForYarnPathCreated(conf);
    createVolume("");
  }

  private void moveHistoryDataToNewVolume(Configuration conf) throws Exception {
    // move data only once - after upgrade, when new HS volume is empty yet
    Path dstPath = new Path(mountPath);
    FileStatus[] stats = fs.listStatus(dstPath);
    if(stats.length > 0) {
      LOG.debug("History data is not moved, HS volume is already not empty: " + Arrays.stream(stats).map(FileStatus::getPath).collect(Collectors.toList()));
      return;
    }
    String rmStagingDir = conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR);

    List<FileStatus> historyData = Arrays.asList(fs.listStatus(new Path(rmStagingDir)))
            .stream()
            .filter(volume -> !volume.getPath().getName().matches(ApplicationId.appIdStrPrefix))
            .collect(Collectors.toList());

    for(FileStatus srcDir: historyData) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("History dir " + srcDir.getPath().toUri().getRawPath() + " is moved to " + dstPath.toUri().getRawPath());
      }
      Path dstDir = new Path(dstPath, srcDir.getPath().getName());
      FileUtil.copy(fs, srcDir, fs, dstDir, false, true, conf);
      this.copyPermissionsIfNeeded(srcDir, dstDir);
      fs.delete(srcDir.getPath(), true);
    }
  }

  private void copyPermissionsIfNeeded(FileStatus srcStatus, Path dst) throws Exception {
    Path src = srcStatus.getPath();

    if (srcStatus.isDirectory()) {
      List<FileStatus> contents = Arrays.asList(fs.listStatus(src));
      for (FileStatus innerFile : contents) {
        copyPermissionsIfNeeded(innerFile, new Path(dst, innerFile.getPath().getName()));
      }
    }
    copyOwnerAndPermission(srcStatus, dst);
  }

  private void copyOwnerAndPermission(FileStatus srcStatus, Path dst) throws Exception {
    FileStatus dstFileStatus = fs.getFileStatus(dst);
    FsPermission srcFilePermission = srcStatus.getPermission();
    String srcFileOwner = srcStatus.getOwner();
    String srcFileGroup = srcStatus.getGroup();
    if(!dstFileStatus.getPermission().equals(srcFilePermission)) {
      fs.setPermission(dst, srcFilePermission);
    }
    if(!dstFileStatus.getOwner().equals(srcFileOwner) || !dstFileStatus.getGroup().equals(srcFileGroup)) {
      fs.setOwner(dst, srcFileOwner, srcFileGroup);
    }
  }
}
