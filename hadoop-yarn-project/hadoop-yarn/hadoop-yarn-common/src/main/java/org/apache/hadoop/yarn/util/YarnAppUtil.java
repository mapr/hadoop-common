/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class YarnAppUtil {

    public static final Logger LOG =
            LoggerFactory.getLogger(YarnAppUtil.class);
    /**
     * File permission to be used for any application specific directory.
     * It is rwx------. This ensures that only the app owner can access it.
     * mapr user is a special user and can therefore still access it.
     */
    public static final FsPermission APP_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0700);

    /**
     * File permission used for any application specific file. It is world-wide
     * readable and owner writable : rw-r--r--. This is needed to make sure the
     * mapr user can read such files. Other users cannot read the file because
     * APP_DIR_PERMISSION disallows any user other than the app owner to access
     * it. mapr user is an exception to this rule.
     */
    final public static FsPermission APP_FILE_PERMISSION =
            FsPermission.createImmutable((short) 0644);

    /**
     * Permission for the system directory on RM. This directory is used only by
     * mapr user. Hence no permission for other and group.
     */
    final public static FsPermission RM_SYSTEM_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0700);

    /**
     * Permission for the staging directory on RM. This directory is used
     * to upload client specific files such as MapR ticket as the client
     * user itself and so needs to be accessible by all users.
     */
    final public static FsPermission RM_STAGING_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0777);

    private static final String MAPR_TICKET_FILE = "ticketfile";

    private static final LocalDirAllocator dirAllocator =
            new LocalDirAllocator(YarnConfiguration.NM_LOCAL_DIRS);

    /**
     * Returns staging dir for the given app on resource manager.
     */
    public static Path getRMStagingDir(String appIdStr,
                                       FileSystem fs, Configuration conf) throws IOException {
        String rmStagingDir = conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR);
        return getRMDirWithVolume(appIdStr, fs, conf, rmStagingDir, true);
    }

    /**
     * Returns staging dir for the given app on resource manager.
     * It does not lookup through all staging directories on all volumes,
     * because is used for "create" activity, not for "read" or "delete"
     */
    public static Path getRMStagingDirForWrite(String appIdStr,
                                               FileSystem fs, Configuration conf) throws IOException {
        String rmStagingDir = conf.get(YarnDefaultProperties.RM_STAGING_DIR, YarnDefaultProperties.DEFAULT_RM_STAGING_DIR);
        return getRMDirWithVolume(appIdStr, fs, conf, rmStagingDir, false);
    }

    /**
     * Returns system dir for the given app on resource manager.
     * It does not lookup through all system directories on all volumes,
     * because is used for "create" activity, not for "read" or "delete"
     */
    public static Path getRMSystemDirForWrite(String appIdStr,
                                              FileSystem fs, Configuration conf) throws IOException {
        String rmSystemDir = conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR);
        return getRMDirWithVolume(appIdStr, fs, conf, rmSystemDir, false);
    }

    /**
     * Returns system dir for the given app on resource manager.
     */
    public static Path getRMSystemDir(String appIdStr,
                                      FileSystem fs, Configuration conf) throws IOException {
        String rmSystemDir = conf.get(YarnDefaultProperties.RM_SYSTEM_DIR, YarnDefaultProperties.DEFAULT_RM_SYSTEM_DIR);
        return getRMDirWithVolume(appIdStr, fs, conf, rmSystemDir, true);
    }

    public static Path getRMDirWithVolume(String appIdStr,
                                          FileSystem fs, Configuration conf, String rmSubDir, boolean lookUpAllDirs) throws IOException {
        String rmDir = conf.get(YarnDefaultProperties.RM_DIR, YarnDefaultProperties.DEFAULT_RM_DIR);
        Path rmDirPath = new Path(rmDir);
        Path rmSubDirPath = new Path(rmSubDir);

        String dirSuffix = rmSubDirPath.toString().substring(rmDirPath.toString().length());

        boolean useVolumeSharding = conf.getBoolean(YarnDefaultProperties.RM_DIR_VOLUME_SHARDING_ENABLED, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_SHARDING_ENABLED)
                && rmSubDirPath.toUri().getRawPath().startsWith(rmDirPath.toUri().getRawPath());
        Path result;
        if (useVolumeSharding) {
            int volumeCount = conf.getInt(YarnDefaultProperties.RM_DIR_VOLUME_COUNT, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_COUNT);
            int rmVolumeName = Math.abs(appIdStr.hashCode() % volumeCount);
            StringBuilder sb = new StringBuilder();
            sb.append(rmDirPath.toUri())
                    .append(Path.SEPARATOR)
                    .append(rmVolumeName)
                    .append(dirSuffix);

            Path dir = new Path(sb.toString());
            result = new Path(fs.makeQualified(dir).toString() + Path.SEPARATOR + appIdStr);
        } else {
            Path dir = new Path(rmSubDir);
            result = new Path(fs.makeQualified(dir).toString() + Path.SEPARATOR + appIdStr);
        }
        if (!fs.exists(result) && lookUpAllDirs) {
            Path allRMDirsPath = inspectAllRMDirs(fs, rmDirPath, dirSuffix, appIdStr);
            if(allRMDirsPath != null && fs.exists(allRMDirsPath)) {
                result = allRMDirsPath;
            }
        }
        return result;
    }

    private static Path inspectAllRMDirs(FileSystem fs, Path rmDirPath, String dirSuffix, String appIdStr) throws IOException {
        List<FileStatus> rmVolumes = Arrays.asList(fs.listStatus(rmDirPath))
                .stream()
                .filter(volume -> volume.getPath().getName().matches("\\d+"))
                .collect(Collectors.toList());
        for (FileStatus volumeStatus : rmVolumes) {
            Path appIdPathWithVolume = new Path(fs.makeQualified(volumeStatus.getPath()).toString() + dirSuffix + Path.SEPARATOR + appIdStr);
            if (fs.exists(appIdPathWithVolume)) {
                return appIdPathWithVolume;
            }
        }
        Path appIdPath = new Path(fs.makeQualified(rmDirPath).toString() + dirSuffix + Path.SEPARATOR + appIdStr);
        if (fs.exists(appIdPath)) {
            return appIdPath;
        }
        LOG.debug("App dir " + dirSuffix + " is not found for app " + appIdStr + ". " +
                "It's fine for application submit and RM recovery");
        return null;
    }

    /**
     * Returns the path of MapR ticket for the given app from the staging
     * directory on resource manager.
     */
    public static Path getRMStagedMapRTicketPath(String appIdStr,
                                                 FileSystem fs, Configuration conf) throws IOException {

        return getMapRTicketPath(getRMStagingDir(appIdStr, fs, conf));
    }

    /**
     * Returns the path of MapR ticket for the given app from the system
     * directory on resource manager.
     */
    public static Path getRMSystemMapRTicketPath(String appIdStr,
                                                 FileSystem fs, Configuration conf) throws IOException {

        return getMapRTicketPath(getRMSystemDir(appIdStr, fs, conf));
    }

    public static Path getMapRTicketPath(Path appDir) {
        return new Path(appDir, MAPR_TICKET_FILE);
    }

    /**
     * Returns the MapR ticket location relative to NodeManager private directory.
     */
    public static String getNMPrivateRelativeTicketLocation(String appIdStr) {
        StringBuilder sb = new StringBuilder();
        sb.append("nmPrivate")
                .append(Path.SEPARATOR)
                .append(appIdStr).append(Path.SEPARATOR)
                .append(MAPR_TICKET_FILE);

        return sb.toString();
    }

    /**
     * Returns the absolute MapR ticket path on NodeManager private directory.
     */
    public static Path getNMPrivateTicketPath(String appIdStr,
                                              Configuration conf) {
        return new Path(conf.get(YarnConfiguration.NM_LOCAL_DIRS),
                getNMPrivateRelativeTicketLocation(appIdStr));
    }

    /**
     * Returns the absolute MapR ticket path on NodeManager private directory.
     * See LocalDirAllocator#getLocalPathForWrite
     */
    public static Path getNMPrivateTicketPathForWrite(String appIdStr,
                                                      Configuration conf) throws IOException {
        return dirAllocator.getLocalPathForWrite(getNMPrivateRelativeTicketLocation(appIdStr), conf);
    }

    /**
     * Returns the absolute MapR ticket path on NodeManager private directory.
     * See LocalDirAllocator#getLocalPathToRead
     */
    public static Path getNMPrivateTicketPathForRead(String appIdStr,
                                                     Configuration conf) throws IOException {
        return dirAllocator.getLocalPathToRead(getNMPrivateRelativeTicketLocation(appIdStr), conf);
    }
}
