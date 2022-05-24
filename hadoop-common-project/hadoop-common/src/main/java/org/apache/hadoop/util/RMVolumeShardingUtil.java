package org.apache.hadoop.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RMVolumeShardingUtil {

    public static final Logger LOG =
            LoggerFactory.getLogger(RMVolumeShardingUtil.class);

    private static final String MAPR_INSTALL_DIR = BaseMapRUtil.getPathToMaprHome();
    private static final String MAPR_RM_VOLUME_SCRIPT_PATH = "/server/createRMVolume.sh";
    private static final String appIdStrPrefix = "application_";


    public static void rebalanceVolumes(String rebalanceDir, int volumeCount, boolean useVolumeSharding, String rmDir, FileSystem fs) throws Exception {
        Path rebalancePath = new Path(rebalanceDir);
        Path rmDirPath = new Path(rmDir);

        String rmAppRootSuffix = rebalancePath.toString().substring(rmDirPath.toString().length());
        List<FileStatus> rmVolumes = null;
        try {
            rmVolumes = Arrays.asList(fs.listStatus(rmDirPath))
                    .stream()
                    .filter(volume -> volume.getPath().getName().matches("\\d+"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.warn("Exception while try to rebalance volumes at " + rmDirPath, e);
        }
        if (rmVolumes == null || rmVolumes.size() == 0) {
            return;
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("Rebalance for rebalanceDir is called: " + rebalanceDir);
        }
        // check if rebalancePath is placed in rmDir
        if (!rebalancePath.toUri().getRawPath().startsWith(rmDirPath.toUri().getRawPath())) {
            LOG.warn("rebalancePath " + rebalancePath.toUri().getRawPath() + " is not the part of rmDir: " + rmDirPath.toUri().getRawPath());
            return;
        }

        Map<Integer, Path> volumeToDirMap = new HashMap<Integer, Path>();
        long maxVolumeNumber = rmVolumes.size();

        for (int volumeNumber = 0; volumeNumber < maxVolumeNumber; volumeNumber++) {
            volumeToDirMap.put(volumeNumber, new Path(rmDir + Path.SEPARATOR + volumeNumber + rmAppRootSuffix));
        }

        // move app data from volumes to central directory if volumes were turned off
        if (!useVolumeSharding) {
            for (Map.Entry<Integer, Path> entry : volumeToDirMap.entrySet()) {
                Integer oldVolume = entry.getKey();
                List<FileStatus> volumeAppIds = Arrays.asList(fs.listStatus(entry.getValue()))
                        .stream()
                        .filter(appIdEntry -> appIdEntry.getPath().getName().startsWith(appIdStrPrefix))
                        .collect(Collectors.toList());
                for (FileStatus appId : volumeAppIds) {
                    fs.rename(appId.getPath(), new Path(appId.getPath().toString().replaceAll(rmDir + Path.SEPARATOR + oldVolume, rmDir)));
                }
            }
        }

        // move app data from central directory to volumes if volumes were turned on
        if (useVolumeSharding) {
            Integer firstVolume = 0;
            String centralDirPath = volumeToDirMap.get(firstVolume).toString().replaceAll(rmDir + Path.SEPARATOR + firstVolume, rmDir);
            List<FileStatus> rmDirAppIds = Arrays.asList(fs.listStatus(new Path(centralDirPath)))
                    .stream()
                    .filter(appIdEntry -> appIdEntry.getPath().getName().startsWith(appIdStrPrefix))
                    .collect(Collectors.toList());
            if (rmDirAppIds.size() > 0) {
                for (FileStatus appId : rmDirAppIds) {
                    Integer newVolume = Math.abs(appId.getPath().getName().hashCode() % volumeCount);
                    fs.rename(appId.getPath(), new Path(appId.getPath().toString().replaceAll(rmDir, rmDir + Path.SEPARATOR + newVolume)));
                }
            }
        }

        Map<Integer, List<FileStatus>> volumeToAppIdMap = new HashMap<Integer, List<FileStatus>>();
        // fill volumeToAppIdMap before start rebalance
        for (Map.Entry<Integer, Path> entry : volumeToDirMap.entrySet()) {
            List<FileStatus> volumeAppIds = Arrays.asList(fs.listStatus(entry.getValue()))
                    .stream()
                    .filter(appIdEntry -> appIdEntry.getPath().getName().startsWith(appIdStrPrefix))
                    .collect(Collectors.toList());
            volumeToAppIdMap.put(entry.getKey(), volumeAppIds);
        }

        // make this iteration separate, in this case we omit twice checks for directories which are already moved
        for (Map.Entry<Integer, List<FileStatus>> entry : volumeToAppIdMap.entrySet()) {
            Integer oldVolume = entry.getKey();
            List<FileStatus> appIds = entry.getValue();
            for (FileStatus appId : appIds) {
                Integer newVolume = Math.abs(appId.getPath().getName().hashCode() % volumeCount);
                if (!newVolume.equals(oldVolume)) {
                    fs.rename(appId.getPath(), new Path(appId.getPath().toString().replaceAll(rmDir + Path.SEPARATOR + oldVolume, rmDir + Path.SEPARATOR + newVolume)));
                }
            }
        }
    }

    public static String getPathToVolumeCreateScript() {
        return MAPR_INSTALL_DIR + MAPR_RM_VOLUME_SCRIPT_PATH;
    }

    public static String getPathToVolumeLog() {
        return MAPR_INSTALL_DIR;
    }
}
