package org.apache.hadoop.yarn.server.applicationhistoryservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.util.YarnAppUtil;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.volume.VolumeManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.conf.YarnDefaultProperties.DEFAULT_RM_STAGING_DIR;
import static org.apache.hadoop.yarn.conf.YarnDefaultProperties.RM_STAGING_DIR;

public class HSVolumeManager extends VolumeManager {

    public static final String HS_WORK_DIR = "yarn.app.mapreduce.am.staging-dir";

    private String rmDir;
    private String rmStagingDir;
    private String hsWorkDir;

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
        rmStagingDir = conf.get(RM_STAGING_DIR, DEFAULT_RM_STAGING_DIR);
        rmDir = conf.get(YarnDefaultProperties.RM_DIR, YarnDefaultProperties.DEFAULT_RM_DIR);
        hsWorkDir = conf.get(HS_WORK_DIR);
        mountPath = conf.get(YarnDefaultProperties.APP_HISTORY_STAGING_DIR, YarnDefaultProperties.DEFAULT_APP_HISTORY_STAGING_DIR);
        volumeMode = "hs";
        volumeLogfilePath = volumeLogfilePath + "/logs/createJHSVolume.log";

        if(newVolumePathSupportEnabled) {
            verifyHSVolumeMountPoint();
            createVolumes(conf);
            fs.setPermission(new Path(mountPath), YarnAppUtil.RM_STAGING_DIR_PERMISSION);

            if(mountPath.equals(hsWorkDir) && !hsWorkDir.equals(rmStagingDir)) {
                moveHistoryData(new Path(rmStagingDir), new Path(mountPath), conf);
            } else {
                moveHistoryData(new Path(mountPath), new Path(rmStagingDir), conf);
            }
        } else {
            waitForRMVolume();
            Map<String, String> hsVolumeInfo = getVolumeInfo(HS_VOLUME_NAME);
            if(hsVolumeInfo != null) {
                String volumeName = hsVolumeInfo.get(VOLUME_NAME);
                String volumePath = hsVolumeInfo.get(VOLUME_PATH);
                int mounted = Integer.parseInt(hsVolumeInfo.get(VOLUME_MOUNTED));

                if(volumeName.equals(HS_VOLUME_NAME)) {
                    if(mounted == 1 && !volumePath.equals(mountPath)) {
                        LOG.info("Volume " + HS_VOLUME_NAME + " already exist and is mounted at " + volumePath + ". Remounting to default path " + mountPath);
                        unmountVolume(HS_VOLUME_NAME);
                    }
                    createVolumes(conf);
                    if(hsWorkDir.equals(rmStagingDir)) {
                        moveHistoryData(new Path(mountPath), new Path(rmStagingDir), conf);
                    } else {
                        moveHistoryData(new Path(rmStagingDir), new Path(mountPath), conf);
                    }
                }
            }
        }
    }

    @Override
    public void createVolumes(Configuration conf) throws Exception {
        waitForYarnPathCreated();
        if(!isVolumeMounted(HS_VOLUME_NAME)) {
            createVolume("");
        }
    }

    private void moveHistoryData(Path srcPath, Path dstPath, Configuration conf) throws Exception {
        waitForRMVolume();
        List<FileStatus> historyData = Arrays.asList(fs.listStatus(srcPath))
                .stream()
                .filter(volume -> !volume.getPath().getName().startsWith(ApplicationId.appIdStrPrefix))
                .collect(Collectors.toList());

        for(FileStatus srcDir: historyData) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("History dir " + srcDir.getPath().toUri().getRawPath() + " is moved to " + dstPath.toUri().getRawPath());
            }
            Path dstDir = new Path(dstPath, srcDir.getPath().getName());
            if(fs.exists(dstDir)) {
                deepCopy(srcDir.getPath(), dstDir, conf);
                if(fs.isDirectory(srcDir.getPath()) && fs.isDirectory(dstDir)) {
                    fs.delete(srcDir.getPath(), true);
                }
            } else {
                copyAsMove(srcDir, dstDir, conf);
            }
        }
    }

    private void deepCopy(Path src, Path dst, Configuration conf) throws Exception {
        if(fs.isDirectory(src) && fs.isDirectory(dst)) {
            LOG.warn(src + " already exists in path " + dst + ", data will be merged");
            FileStatus[] srcFiles = fs.listStatus(src);
            for(FileStatus srcFileStatus: srcFiles) {
                Path dstFile = new Path(dst, srcFileStatus.getPath().getName());
                if(fs.exists(dstFile)) {
                    deepCopy(srcFileStatus.getPath(), dstFile, conf);
                    fs.delete(srcFileStatus.getPath(), true);
                } else {
                    LOG.info(srcFileStatus.getPath() + " is moved to " + dstFile);
                    copyAsMove(srcFileStatus, dstFile, conf);
                }
            }
        } else {
            FileStatus srcFileStatus = fs.getFileStatus(src);
            if(fs.exists(dst)) {
                FileStatus dstFileStatus = fs.getFileStatus(dst);
                long srcFileModTime = srcFileStatus.getModificationTime();
                long dstFileModTime = dstFileStatus.getModificationTime();
                if(srcFileModTime > dstFileModTime) {
                    LOG.info(srcFileStatus.getPath() + " is replacing " + dst + " due most recent modification time");
                    LOG.debug( srcFileStatus.getPath()  + " modification time = " + srcFileModTime
                            + dst + "modification time = " + dstFileModTime);
                    fs.delete(dst, true);
                    copyAsMove(srcFileStatus, dst, conf);
                } else {
                    LOG.info(dst + " is not replaced by " + srcFileStatus.getPath() + " due most recent modification time");
                    LOG.debug(srcFileStatus.getPath()  + " modification time = " + srcFileModTime
                            + dst + "modification time = " + dstFileModTime);
                    fs.delete(srcFileStatus.getPath(), true);
                }
            } else {
                LOG.info(srcFileStatus.getPath() + " is moved to " + dst);
                copyAsMove(srcFileStatus, dst, conf);
            }
        }
    }

    private void copyAsMove(FileStatus srcFile, Path dstFile, Configuration conf) throws Exception {
        FileUtil.copy(fs, srcFile, fs, dstFile, false, true, conf);
        copyPermissionsIfNeeded(srcFile, dstFile);
        fs.delete(srcFile.getPath(), true);
    }

    private void waitForRMVolume() throws Exception {
        int waitTimeTotal = 600;
        int waitTime = 0;
        Path rmStagingDirPath = new Path(rmStagingDir);
        while(!isRMVolumeMounted()) {
            LOG.info("Waiting for RM volume mapr.resourcemanager.volume mount finish");
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("HSVolumeManager launch failed, mapr.resourcemanager.volume is not mounted or mount point is incorrect");
            }
        }
        while(!fs.exists(rmStagingDirPath)) {
            LOG.info("Waiting for RM staging directory creation");
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("HSVolumeManager launch failed, staging directory " + rmStagingDir + " does not exist");
            }
        }
        Path oldRMDir = new Path(rmDir + "/rm");
        while(fs.exists(oldRMDir) && fs.listStatus(oldRMDir).length > 0) {
            LOG.info("Waiting for RM data migration to new volume");
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("HSVolumeManager launch failed, data migration from old RM volume " + oldRMDir + " is not finished");
            }
        }
    }

    private boolean isRMVolumeMounted() {
        try {
            Map<String, String> volumeInfo = getVolumeInfo(RM_VOLUME_NAME);
            if(volumeInfo != null) {
                String volumeName = volumeInfo.get(VOLUME_NAME);
                String rmVolumePath = volumeInfo.get(VOLUME_PATH);
                int mounted = Integer.parseInt(volumeInfo.get(VOLUME_MOUNTED));
                if(volumeName.equals(RM_VOLUME_NAME) && mounted == 1 && rmVolumePath.equals(rmDir)) {
                    return true;
                } else if (!newVolumePathSupportEnabled && rmVolumePath.equals(new Path(rmDir).getParent().toUri().getRawPath()) && volumeName.equals(RM_VOLUME_NAME) && mounted == 1) {
                    return true;
                }
            }
        } catch (IOException e) {
            LOG.error("", e);
        }
        return false;
    }

    private void verifyHSVolumeMountPoint() throws IOException {
        Map<String, String> volumeInfo = getVolumeInfo(HS_VOLUME_NAME);
        if(volumeInfo != null) {
            String volumeName = volumeInfo.get(VOLUME_NAME);
            String hsVolumePath = volumeInfo.get(VOLUME_PATH);
            int mounted = Integer.parseInt(volumeInfo.get(VOLUME_MOUNTED));
            if(!hsVolumePath.equals(mountPath) && volumeName.equals(HS_VOLUME_NAME) && mounted == 1) {
                LOG.info("Volume " + HS_VOLUME_NAME + " is mounted at " + hsVolumePath + ". Mount path is configured as " + mountPath);
                unmountVolume(HS_VOLUME_NAME);
            }
        }
    }
}
