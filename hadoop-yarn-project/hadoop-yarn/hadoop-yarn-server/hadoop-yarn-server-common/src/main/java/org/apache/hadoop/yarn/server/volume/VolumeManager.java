package org.apache.hadoop.yarn.server.volume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BaseMapRUtil;
import org.apache.hadoop.util.RMVolumeShardingUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.server.api.ConfigurableAuxiliaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class VolumeManager extends ConfigurableAuxiliaryService {

    protected Logger LOG = LoggerFactory.getLogger(VolumeManager.class);
    protected String volumeLogfilePath;
    protected String volumeMode;
    protected String mountPath;
    protected boolean newVolumePathSupportEnabled;
    protected FileSystem fs;

    public VolumeManager(String name) {
        super(name);
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        fs = FileSystem.get(conf);
        newVolumePathSupportEnabled = conf.getBoolean(YarnDefaultProperties.RM_DIR_VOLUME_NEW_PATH_SUPPORT_ENABLED, YarnDefaultProperties.DEFAULT_RM_DIR_VOLUME_NEW_PATH_SUPPORT_ENABLED);
        volumeLogfilePath = RMVolumeShardingUtil.getPathToVolumeLog();
    }

    public abstract void createVolumes(Configuration conf) throws Exception;

    protected void createVolume(String volumeNumber) throws Exception {
        String pathToVolumeScript = RMVolumeShardingUtil.getPathToVolumeCreateScript();

        int argsCount = !volumeNumber.equals("") ? 6 : 5;

        String[] args = new String[argsCount];
        args[0] = pathToVolumeScript;
        args[1] = BaseMapRUtil.getMapRHostName();
        args[2] = mountPath;
        args[3] = mountPath;
        args[4] = volumeMode;  // To distinguish RM from JHS
        if(argsCount > 5) {
            args[5] = volumeNumber;
        }
        String volumePath = (argsCount > 5) ? args[2] + Path.SEPARATOR + args[5] : args[2];

        // Set MAPR_MAPREDUCE_MODE to "yarn" since this is in ResourceManager and
        // hadoop commands invoked should be with the hadoop2 script
        Map<String, String> env = new HashMap<String, String>();
        env.put("MAPR_MAPREDUCE_MODE", "yarn");

        Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(args, null, env);
        if (LOG.isInfoEnabled())
            LOG.info("Checking for volume." +
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
                        LOG.error("Failed to create and mount volume at "
                                + volumePath + ". Please see logs at " + volumeLogfilePath);

                        LOG.error("Command ran " + shexec.toString());
                        LOG.error("Command output " + shexec.getOutput());
                    }
                    throw ioe;
                } else {
                    // Wait and retry
                    Thread.sleep(100);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Retrying check for volume ... ");
                    }
                }
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Sucessfully created volume and mounted at "
                    + volumePath);
        }
    }

    protected void waitForYarnPathCreated(Configuration conf) throws Exception {
        int timeout = 200;
        Path yarnDir = new Path(conf.get(YarnDefaultProperties.YARN_DIR, YarnDefaultProperties.DEFAULT_YARN_DIR));
        Path yarnParentDir = yarnDir.getParent();
        if(!fs.exists(yarnParentDir)) {
            LOG.info("Waiting for Yarn parent directory creation("+ yarnParentDir.toString() +") before mounting RM volumes");
        }
        for(int i = 0; i < timeout; i++) {
            if(!fs.exists(yarnParentDir)) {
                TimeUnit.SECONDS.sleep(3);
            } else {
                break;
            }
        }
        if(!fs.exists(yarnParentDir)) {
            throw new RuntimeException("Yarn parent directory is not found: " + yarnParentDir.toString());
        }
        verifyYarnDirPermissions(yarnDir);
        if(newVolumePathSupportEnabled) {
            fs.mkdirs(yarnDir);
        }
    }

    protected void createDir(String pathName,
                             FsPermission perm) throws IOException {

        if (LOG.isInfoEnabled()) {
            LOG.info("Creating dir: " + pathName + " with permission: " + perm);
        }
        FileSystem.mkdirs(fs, new Path(pathName), perm);
    }

    protected void verifyYarnDirPermissions(Path yarnDir) throws Exception {
        Path clusterDirectory = yarnDir.getParent();
        Path parentClusterDirectory = clusterDirectory.getParent();

        if(fs.exists(parentClusterDirectory)) {
            FileStatus parentClusterDirectoryFileStatus = fs.getFileStatus(parentClusterDirectory);
            if(fs.exists(clusterDirectory)) {
                copyOwnerAndPermission(parentClusterDirectoryFileStatus, clusterDirectory);
            }
            if(fs.exists(yarnDir)) {
                copyOwnerAndPermission(parentClusterDirectoryFileStatus, yarnDir);
            }
        }
    }

    protected void copyOwnerAndPermission(FileStatus srcStatus, Path dst) throws Exception {
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