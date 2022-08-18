/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.gson.JsonArray;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.rpcauth.RpcAuthRegistry;
import org.apache.hadoop.util.MaprShellCommandExecutor;
import org.apache.hadoop.yarn.util.ScramCredentialScriptUtil;
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
    private String newHSVolumeMountPath;

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

        try {
            lockVolume(RM_VOLUME_NAME);
            // unmount if necessary
            verifyRMVolumeMountPoint();
            if(useVolumeSharding) {
                verifyRMShardedVolumesMountPoints();
            }
            createVolumes(conf);
            if(newHSVolumeMountPath != null && !isVolumeMounted(HS_VOLUME_NAME)) {
                mountVolume(HS_VOLUME_NAME, newHSVolumeMountPath);
            }
            RMVolumeShardingUtil.rebalanceVolumes(rmSystemDir, volumeCount, useVolumeSharding, rmDir, fs);
            RMVolumeShardingUtil.rebalanceVolumes(rmStagingDir, volumeCount, useVolumeSharding, rmDir, fs);
        } finally {
            unlockVolume(RM_VOLUME_NAME);
        }
    }

    @Override
    public void createVolumes(Configuration conf) throws Exception {
        waitForYarnPathCreated();
        // create separate volume for general RM dir
        if(!isVolumeMounted(RM_VOLUME_NAME)) {
            createVolume("");
            if (newVolumePathSupportEnabled) {
                moveVolumeDataAfterUpgrade();
            }
        }
        if(conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_TOKEN_MECHANISM, UserGroupInformation.DIGEST_AUTH_MECHANISM).
                equalsIgnoreCase(UserGroupInformation.SCRAM_AUTH_MECHANISM)){
            ScramCredentialScriptUtil.checkAndCopyScramCreds(conf, "resourceManager");
        }
        createDir(conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR),
                YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

        createDir(conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR),
                YarnAppUtil.RM_STAGING_DIR_PERMISSION);

        if(useVolumeSharding) {
            for (int volumeNumber = 0; volumeNumber < volumeCount; volumeNumber++) {
                if(!isVolumeMounted(RM_VOLUME_NAME + "_" + volumeNumber)) {
                    createVolume(Integer.toString(volumeNumber));
                }
                createDir(rmSystemDir.replaceAll(rmDir, rmDir + Path.SEPARATOR + volumeNumber),
                        YarnAppUtil.RM_SYSTEM_DIR_PERMISSION);

                createDir(rmStagingDir.replaceAll(rmDir, rmDir + Path.SEPARATOR + volumeNumber),
                        YarnAppUtil.RM_STAGING_DIR_PERMISSION);
            }
        }
    }

    private void verifyRMVolumeMountPoint() throws IOException {
        Map<String, String> volumeInfo = getVolumeInfo(RM_VOLUME_NAME);
        if(volumeInfo != null) {
            String rmVolumePath = volumeInfo.get(VOLUME_PATH);
            int mounted = Integer.parseInt(volumeInfo.get(VOLUME_MOUNTED));
            if(mounted != 1) {
                return;
            }
            // newVolumePathSupportEnabled disabled, while volume already remounted to new path
            if(rmVolumePath.equals(rmDir) && !newVolumePathSupportEnabled) {
                LOG.warn("Volume " + RM_VOLUME_NAME + " is mounted at " + rmDir + ". Disabling for property " +
                        YarnDefaultProperties.RM_DIR_VOLUME_NEW_PATH_SUPPORT_ENABLED + " is not supported. Continue as new-volume-path-support is true");
                // newVolumePathSupportEnabled enabled, remount required, but HS volume should be checked first
            } else if(!rmVolumePath.equals(mountPath)) {
                if(isHSVolumeChildForRMVolume(rmVolumePath)) {
                    unmountVolume(HS_VOLUME_NAME);
                }
                LOG.info("Volume " + RM_VOLUME_NAME + " is mounted at " + rmVolumePath + ". Mount path is configured as " + mountPath);
                unmountVolume(RM_VOLUME_NAME);
            }
        }
    }

    private void verifyRMShardedVolumesMountPoints() throws IOException {
        for (int volumeNumber = 0; volumeNumber < volumeCount; volumeNumber++) {
            String shardedVolumeName = RM_VOLUME_NAME + "_" + volumeNumber;
            String volumePath = rmDir + Path.SEPARATOR + volumeNumber;
            Map<String, String> volumeInfo = getVolumeInfo(shardedVolumeName);
            if (volumeInfo != null) {
                String mountedPath = volumeInfo.get(VOLUME_PATH);
                int mounted = Integer.parseInt(volumeInfo.get(VOLUME_MOUNTED));
                if (mounted != 1) {
                    return;
                }
                if(!mountedPath.equals(volumePath)) {
                    unmountVolume(shardedVolumeName);
                }
            }
        }
    }

    private boolean isHSVolumeChildForRMVolume(String rmVolumePath) throws IOException {
        Map<String, String> volumeInfo = getVolumeInfo(HS_VOLUME_NAME);
        if(volumeInfo != null) {
            String hsVolumePath = volumeInfo.get(VOLUME_PATH);
            int mounted = Integer.parseInt(volumeInfo.get(VOLUME_MOUNTED));
            if(hsVolumePath.startsWith(rmVolumePath + Path.SEPARATOR) && mounted == 1) {
                newHSVolumeMountPath = hsVolumePath;
                LOG.info("Volume " + HS_VOLUME_NAME + " with path " + hsVolumePath + " is child of " + RM_VOLUME_NAME + " with path " + rmVolumePath + "" +
                        ". Before unmount " + RM_VOLUME_NAME + " need to unmount " + HS_VOLUME_NAME);
                return true;
            }
        }
        return false;
    }

    private void moveVolumeDataAfterUpgrade() throws IOException {
        Path oldRMDir = new Path(mountPath + "/rm");
        if(fs.exists(oldRMDir)) {
            FileStatus[] oldData = fs.listStatus(oldRMDir);
            for(FileStatus srcDir: oldData) {
                Path dstDir = new Path(mountPath, srcDir.getPath().getName());
                if(!fs.exists(dstDir)) {
                    fs.rename(srcDir.getPath(), dstDir);
                }
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
