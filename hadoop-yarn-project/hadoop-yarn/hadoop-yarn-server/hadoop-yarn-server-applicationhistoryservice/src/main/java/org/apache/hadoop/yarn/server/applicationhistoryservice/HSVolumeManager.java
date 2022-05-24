package org.apache.hadoop.yarn.server.applicationhistoryservice;

import com.google.gson.JsonArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.MaprShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.util.YarnAppUtil;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.volume.VolumeManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HSVolumeManager extends VolumeManager {

    private String rmDir;

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
        rmDir = conf.get(YarnDefaultProperties.RM_DIR, YarnDefaultProperties.DEFAULT_RM_DIR);

        mountPath = conf.get(YarnDefaultProperties.APP_HISTORY_STAGING_DIR, YarnDefaultProperties.DEFAULT_APP_HISTORY_STAGING_DIR);
        volumeMode = "hs";
        volumeLogfilePath = volumeLogfilePath + "/logs/createJHSVolume.log";
        verifyVolumeMountPoint();
        createVolumes(conf);
        fs.setPermission(new Path(mountPath), YarnAppUtil.RM_STAGING_DIR_PERMISSION);
        this.moveHistoryDataToNewVolume(conf);
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
        Path rmStagingDir = new Path(conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR));
        waitForRMVolume(rmStagingDir);
        List<FileStatus> historyData = Arrays.asList(fs.listStatus(rmStagingDir))
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

    private void waitForRMVolume(Path rmStagingDir) throws Exception {
        int waitTimeTotal = 600;
        int waitTime = 0;
        while(!isRMVolumeMounted()) {
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("First HSVolumeManager launch failed, mapr.resourcemanager.volume is not mounted or mount point is incorrect");
            }
        }
        while(!fs.exists(rmStagingDir)) {
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("First HSVolumeManager launch failed, staging directory " + rmStagingDir + " does not exist");
            }
        }
        Path oldRMDir = new Path(rmDir + "/rm");
        while(fs.exists(oldRMDir) && fs.listStatus(oldRMDir).length > 0) {
            TimeUnit.SECONDS.sleep(10);
            waitTime+=10;
            if (waitTime > waitTimeTotal) {
                throw new RuntimeException("First HSVolumeManager launch failed, data migration from old RM volume " + oldRMDir + " is not finished");
            }
        }
    }

    private boolean isRMVolumeMounted() {
        String rmVolumeName = "mapr.resourcemanager.volume";
        MaprShellCommandExecutor executor = new MaprShellCommandExecutor();

        String[] volumeListCommand = new String[] {"volume", "list"};
        Map<String, String> volumeListParams = new HashMap<>();
        volumeListParams.put("columns", "volumename,mountdir,mounted");
        volumeListParams.put("filter", "[n=="+ rmVolumeName +"]");
        try {
            JsonArray result = executor.execute(volumeListCommand, volumeListParams, false);
            if(result != null && result.size() > 0) {
                String volumeName = result.get(0).getAsJsonObject().get("volumename").getAsString();
                int mounted = result.get(0).getAsJsonObject().get("mounted").getAsInt();
                String rmVolumePath = result.get(0).getAsJsonObject().get("mountdir").getAsString();

                if(volumeName.equals(rmVolumeName) && mounted == 1 && rmVolumePath.equals(rmDir)) {
                    return true;
                } else if (!newVolumePathSupportEnabled && rmVolumePath.equals(new Path(rmDir).getParent().toUri().getRawPath()) && volumeName.equals(rmVolumeName) && mounted == 1) {
                    return true;
                }
            }
        } catch (IOException e) {
            LOG.error("", e);
        }
        return false;
    }

    private void verifyVolumeMountPoint() throws IOException {
        String hsVolumeName = "mapr.historyserver.volume";
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
            if(!hsVolumePath.equals(mountPath) && volumeName.equals(hsVolumeName) && mounted == 1) {
                LOG.info("Volume " + hsVolumeName + " is mounted at " + hsVolumePath + ". Mount path is configured as " + mountPath);
                String[] volumeUnmountCommand = new String[] {"volume", "unmount"};
                Map<String, String> volumeUnmountParams = new HashMap<>();
                volumeUnmountParams.put("name", hsVolumeName);
                executor.execute(volumeUnmountCommand, volumeUnmountParams, false);
            }
        }
    }
}
