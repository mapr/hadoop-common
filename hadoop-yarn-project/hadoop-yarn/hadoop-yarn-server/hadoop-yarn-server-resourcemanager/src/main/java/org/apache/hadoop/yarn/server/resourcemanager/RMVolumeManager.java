/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.gson.JsonArray;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.MaprShellCommandExecutor;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.util.YarnAppUtil;
import org.apache.hadoop.util.RMVolumeShardingUtil;
import org.apache.hadoop.yarn.server.volume.VolumeManager;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

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
        mountPath = newVolumePathSupportEnabled ? rmDir : new Path(rmDir).getParent().toUri().getRawPath();
        rmSystemDir = conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR);
        rmStagingDir = conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR);
        volumeCount = conf.getInt(YarnDefaultProperties.RM_DIR_VOLUME_COUNT, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_COUNT);
        useVolumeSharding = conf.getBoolean(YarnDefaultProperties.RM_DIR_VOLUME_SHARDING_ENABLED, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_SHARDING_ENABLED)
                && new Path(rmStagingDir).toUri().getRawPath().startsWith(new Path(rmDir).toUri().getRawPath())
                && new Path(rmSystemDir).toUri().getRawPath().startsWith(new Path(rmDir).toUri().getRawPath());

        verifyRMVolumeMountPoint();
        createVolumes(conf);
        if(newVolumePathSupportEnabled) {
            moveVolumeDataAfterUpgrade();
        }
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

    private void verifyRMVolumeMountPoint() throws IOException {
        String rmVolumeName = "mapr.resourcemanager.volume";
        MaprShellCommandExecutor executor = new MaprShellCommandExecutor();

        String[] volumeListCommand = new String[] {"volume", "list"};
        Map<String, String> volumeListParams = new HashMap<>();
        volumeListParams.put("columns", "volumename,mountdir,mounted");
        volumeListParams.put("filter", "[n=="+ rmVolumeName +"]");

        JsonArray result = executor.execute(volumeListCommand, volumeListParams, false);
        if(result != null && result.size() > 0) {
            String volumeName = result.get(0).getAsJsonObject().get("volumename").getAsString();
            String rmVolumePath = result.get(0).getAsJsonObject().get("mountdir").getAsString();
            int mounted = result.get(0).getAsJsonObject().get("mounted").getAsInt();
            if(!rmVolumePath.equals(mountPath) && volumeName.equals(rmVolumeName) && mounted == 1 && newVolumePathSupportEnabled) {
                verifyHSVolumeMountPoint(rmVolumePath);
                LOG.info("Volume " + rmVolumeName + " is mounted at " + rmVolumePath + ". Mount path is configured as " + mountPath);
                String[] volumeUnmountCommand = new String[] {"volume", "unmount"};
                Map<String, String> volumeUnmountParams = new HashMap<>();
                volumeUnmountParams.put("name", rmVolumeName);
                executor.execute(volumeUnmountCommand, volumeUnmountParams, false);
            }
        }
    }

    private void verifyHSVolumeMountPoint(String rmVolumePath) throws IOException {
        String hsVolumeName = "mapr.historyserver.volume";
        String rmVolumeName = "mapr.resourcemanager.volume";

        MaprShellCommandExecutor executor = new MaprShellCommandExecutor();

        String[] volumeListCommand = new String[] {"volume", "list"};
        Map<String, String> volumeListParams = new HashMap<>();
        volumeListParams.put("columns", "volumename,mountdir,mounted");
        volumeListParams.put("filter", "[n=="+ hsVolumeName +"]");

        JsonArray result = executor.execute(volumeListCommand, volumeListParams, false);
        if(result != null && result.size() > 0) {
            String volumeName = result.get(0).getAsJsonObject().get("volumename").getAsString();
            String hsVolumePath = result.get(0).getAsJsonObject().get("mountdir").getAsString();
            int mounted = result.get(0).getAsJsonObject().get("mounted").getAsInt();
            if(hsVolumePath.startsWith(rmVolumePath + Path.SEPARATOR) && volumeName.equals(hsVolumeName) && mounted == 1) {
                LOG.info("Volume " + hsVolumeName + " with path " + hsVolumePath + " is child of " + rmVolumeName + " with path " + rmVolumePath + "" +
                        ". Before unmount " + rmVolumeName + " need to unmount " + hsVolumeName);
                String[] volumeUnmountCommand = new String[] {"volume", "unmount"};
                Map<String, String> volumeUnmountParams = new HashMap<>();
                volumeUnmountParams.put("name", hsVolumeName);
                executor.execute(volumeUnmountCommand, volumeUnmountParams, false);
            }
        }
    }

    private void moveVolumeDataAfterUpgrade() throws IOException {
        Path oldRMDir = new Path(mountPath + "/rm");
        if(fs.exists(oldRMDir)) {
            FileStatus[] oldData = fs.listStatus(oldRMDir);
            for(FileStatus srcDir: oldData) {
                Path dstDir = new Path(mountPath, srcDir.getPath().getName());
                fs.rename(srcDir.getPath(), dstDir);
            }
            oldData = fs.listStatus(oldRMDir);
            if(oldData.length == 0) {
                fs.delete(oldRMDir, true);
            } else {
                LOG.warn(oldRMDir + " directory not empty, deletion postponed");
            }
        }
    }
}
