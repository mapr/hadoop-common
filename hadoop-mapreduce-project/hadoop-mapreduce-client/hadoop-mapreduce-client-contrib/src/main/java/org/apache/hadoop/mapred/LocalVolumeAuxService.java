/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */

package org.apache.hadoop.mapred;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.util.ScramCredentialScriptUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.maprfs.AbstractMapRFileSystem;
import org.apache.hadoop.maprfs.MapRPathId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.BaseMapRUtil;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;

import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_LOCALOUTPUT_DIR_PARAM;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_LOCALSPILL_DIR_PARAM;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_LOCALOUTPUT_DIR_DEFAULT;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_LOCALSPILL_DIR_DEFAULT;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_UNCOMPRESSED_SUFFIX;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_MAPRED_LOCAL_VOLUME_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.NodeLocalVolumeMonitorImpl.MAPR_NM_LOCAL_VOLUME_PATH;

/**
 * Auxiliary service to manage local volume that stores job related data.
 */
public class LocalVolumeAuxService extends AuxiliaryService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalVolumeAuxService.class);

  private static final String SERVICE_NAME = DirectShuffleMetaData.DIRECT_SHUFFLE_SERVICE_ID;

  private static final String MAPR_INSTALL_DIR = BaseMapRUtil.getPathToMaprHome();
  private static final String LOCALHOSTNAME = BaseMapRUtil.getMapRHostName();

  private static final String LOCAL_VOLUME_CREATE_SCRIPT_PATH = "/server/createLocalVolumes.sh";
  private static final String LOCAL_VOLUME_CREATE_SCRIPT_LOGFILE_PATH = "/logs/createNMVolume.log";

  // used by the script that creates the local volume
  private static final String YARN_ARG = "yarn";

  // used by the script to create staging volume
  private static final String STAGING_ARG = "staging";

  // used by the script to create staging volume
  private static final String SPARK_ARG = "spark";

  private static final String VOLUME_HEALTH_CHECK_INTERVAL =
      "mapreduce.volume.healthcheck.interval";

  private static final String SPILL_EXPIRATION_DATE =  "mapr.localspill.expiration.date";

  /**
   * Priority for the {@link LocalVolumeAuxServiceShutDownHook}. This should be
   * higher than {@link NodeManager#SHUTDOWN_HOOK_PRIORITY} so that {@link ShutdownHookManager}
   * invokes {@link LocalVolumeAuxServiceShutDownHook} before invoking {@link NodeManager#nodeManagerShutdownHook}.
   */
  public static final int LOCAL_VOL_SERVICE_SHUTDOWN_HOOK_PRIORITY =
      NodeManager.SHUTDOWN_HOOK_PRIORITY < Integer.MAX_VALUE - 10
          ? NodeManager.SHUTDOWN_HOOK_PRIORITY + 10 : Integer.MAX_VALUE;


  enum FidId {
    ROOT,                                   /* nodeManager                   */
    OUTPUT,                                 /* nodeManager/outputs/<jobid>   */
    OUTPUT_U,                               /* nodeManager/outputs.U/<jobid> */
    SPILL,                                  /* nodeManager/spills/<jobid>    */
    SPILL_U                                 /* nodeManager/spills.U/<jobid>  */
  }

  private String[] fidRoots;
  private String[] rootDirNames;
  private Configuration conf;
  private FileSystem maprfs;
  protected MapRDirectShuffleMetaData metaData;
  protected final Map<String, MapRDirectShuffleMetaData> jobMetaData = new ConcurrentHashMap<String, MapRDirectShuffleMetaData>();
  private volatile boolean isShuttingDown = false;

  private final ReentrantReadWriteLock healthLock = new ReentrantReadWriteLock();
  private final Lock readLock = healthLock.readLock();
  private final Lock writeLock = healthLock.writeLock();

  /**
   * Service to check the health of local volume. A scheduled executor is used
   * to periodically run the health check.
   */
  private ScheduledExecutorService volumeChecker;

  /**
   * ScheduledThreadPool to delete localvolume data after job is done
   */
  private ScheduledThreadPoolExecutor deletionService;

  /**
   * Volume check interval (in milliseconds).
   */
  private int volumeCheckInterval = 60 * 1000;

  /**
   * Spill data expiration date (in days)
   */
  private int spillExpirationDate = 30;

  protected LocalVolumeAuxService() {
    super(SERVICE_NAME);
    this.metaData = new MapRDirectShuffleMetaData();
    this.volumeChecker = Executors.newSingleThreadScheduledExecutor();
  }

  private class LocalVolumeAuxServiceShutDownHook implements Runnable {
    private final LocalVolumeAuxService service;

    private LocalVolumeAuxServiceShutDownHook(LocalVolumeAuxService service) {
      this.service = service;
    }

    @Override
    public void run() {
      service.isShuttingDown = true;
    }
  }

  /**
   * Task to check the health of local volume. If it encounters any error during
   * the check, it tries to re-initialize the volume. This is meant to handle
   * cases where a volume has been deleted and so needs to be recreated.
   * If the volume initialization also fails, then this attempt will end. In the
   * next scheduled attempt, the same check will be done.
   */
  private class VolumeHealthCheckTask implements Runnable {
    public void run() {
      final Path volumePath = new Path(getMapRedLocalVolumeMountPath());
      final Path nmPath = new Path(getNodeManagerDirPath());
      try {
        LOG.debug("Checking mapreduce volume {}", volumePath);

        maprfs.getFileStatus(volumePath);

        LOG.debug("Done checking mapreduce volume {}", volumePath);

        maprfs.getFileStatus(nmPath);
        maprfs.getFileStatus(new Path(nmPath, rootDirNames[FidId.OUTPUT.ordinal()]));
        maprfs.getFileStatus(new Path(nmPath, rootDirNames[FidId.OUTPUT_U.ordinal()]));
        maprfs.getFileStatus(new Path(nmPath, rootDirNames[FidId.SPILL.ordinal()]));
        maprfs.getFileStatus(new Path(nmPath, rootDirNames[FidId.SPILL_U.ordinal()]));
        maprfs.getFileStatus(new Path(nmPath, "fidservers"));
      } catch (Exception e) {
        LOG.error("Failed to get status of mapreduce volume " + volumePath + " or it's subdirectories." +
            " Trying to recover", e);
        try {
          initVolume();
        } catch (Exception innerEx) {
          // Suppress exceptions. Otherwise, the task won't be scheduled
          // anymore. Also, no need to log the exception as it is done in
          // initVolume.
          LOG.warn("Exception is thrown while trying to recover local volume." +
              " If exception persists for considerable amount of time you may need to restart NM");
        }
      }
    }
  }

  @VisibleForTesting
  protected void setFS(FileSystem fs) {
    this.maprfs = fs;
  }

  @VisibleForTesting
  protected void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if (LOCALHOSTNAME == null) {
      throw new ServiceStateException("Cannot initialize " + SERVICE_NAME +
          ". Error obtaining hostname from " + BaseMapRUtil.HOST_NAME_FILE_PATH + ".");
    }
    this.conf = conf;
    // to accommodate testing
    if (maprfs == null) {
      this.maprfs = FileSystem.get(conf);
    }
    rootDirNames = new String[5];
    rootDirNames[FidId.ROOT.ordinal()] = ".";
    String outputDirName = conf.get(MAPR_LOCALOUTPUT_DIR_PARAM, MAPR_LOCALOUTPUT_DIR_DEFAULT);
    rootDirNames[FidId.OUTPUT.ordinal()] = outputDirName;
    String outputUDirName = outputDirName + MAPR_UNCOMPRESSED_SUFFIX;
    rootDirNames[FidId.OUTPUT_U.ordinal()] = outputUDirName;
    String spillDirName = getSpillDirName();
    rootDirNames[FidId.SPILL.ordinal()] = spillDirName;
    String spillUDirName = spillDirName + MAPR_UNCOMPRESSED_SUFFIX;
    rootDirNames[FidId.SPILL_U.ordinal()] = spillUDirName;

    this.metaData.setNodeManageHostName(LOCALHOSTNAME);
    this.volumeCheckInterval = conf.getInt(VOLUME_HEALTH_CHECK_INTERVAL,
        this.volumeCheckInterval);

    spillExpirationDate = conf.getInt(SPILL_EXPIRATION_DATE, spillExpirationDate);

    int corePool = YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT;
    if (conf != null) {
      corePool = conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT,
          corePool);
    }
    deletionService = new ScheduledThreadPoolExecutor(corePool);
    deletionService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    deletionService.setKeepAliveTime(60L, TimeUnit.SECONDS);

    // Register a shutdown hook with priority greater than that of the NodeManager. This ensures that
    // the LocalVolumeService becomes aware of the NM process being shutdown *before*
    // {@link LocalVolumeAuxService#stopApplication} is invoked.
    ShutdownHookManager.get().addShutdownHook(
        new LocalVolumeAuxServiceShutDownHook(this), LOCAL_VOL_SERVICE_SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Actions called during the INITED to STARTED transition.
   * <p/>
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   * <p/>
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   * <p>
   * Initializes local volume and starts off a periodic volume health check
   * task with an initial delay equal to the volumeCheckInterval as we don't
   * want the check to happen immediately.
   *
   * @throws Exception if needed -these will be caught,
   *                   wrapped, and trigger a service stop
   */
  @Override
  protected void serviceStart() throws Exception {
    initVolume();
    volumeChecker.scheduleAtFixedRate(new VolumeHealthCheckTask(),
        volumeCheckInterval, volumeCheckInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Calls an external script to creates a local volume for mapreduce.
   */
  private void initVolume() throws IOException {
    final String[] mapReduceVolumeArgs = new String[] {
        MAPR_INSTALL_DIR + LOCAL_VOLUME_CREATE_SCRIPT_PATH,
        // hostname
        LOCALHOSTNAME,
        // vol mount path
        getMapRedLocalVolumeMountPath(),
        // full path
        getNodeManagerDirPath(),
        // yarn or not
        YARN_ARG
    };

    final String[] stagingVolumeArgs = new String[] {
        MAPR_INSTALL_DIR + LOCAL_VOLUME_CREATE_SCRIPT_PATH,
        // hostname
        LOCALHOSTNAME,
        // vol mount path
        getStagingLocalVolumeMountPath(),
        // full path
        getStagingLocalVolumeMountPath() + "/nodeManager",
        // staging volume
        STAGING_ARG
    };

    // Set the MAPR_MAPREDUCE_MODE to "yarn" since this is in NodeManager
    // and all hadoop commands invoked should invoke the hadoop2 script
    Map<String, String> env = new HashMap<>();
    env.put("MAPR_MAPREDUCE_MODE", "yarn");

    executeCommand(mapReduceVolumeArgs, env);
    executeCommand(stagingVolumeArgs, env);
    if(conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_TOKEN_MECHANISM, UserGroupInformation.DIGEST_AUTH_MECHANISM).
        equalsIgnoreCase(UserGroupInformation.SCRAM_AUTH_MECHANISM)){
      ScramCredentialScriptUtil.checkAndCopyScramCreds(conf, "nodeManager");
    }

    if (conf.getBoolean(YarnConfiguration.NM_CREATE_SPARK_VOLUME, YarnConfiguration.DEFAULT_NM_CREATE_SPARK_VOLUME)){
      final String[] sparkVolumeArgs = new String[] {
              MAPR_INSTALL_DIR + LOCAL_VOLUME_CREATE_SCRIPT_PATH,
              // hostname
              LOCALHOSTNAME,
              // vol mount path
              getSparkLocalVolumeMountPath(),
              // full path
              getSparkLocalVolumeMountPath() + "/nodeManager",
              // spark volume
              SPARK_ARG
      };

      executeCommand(sparkVolumeArgs, env);
    }

    try {
      initMapReduceDirs();
    } catch (IOException ioe) {
      LOG.error("Could not initialize directories for mapreduce", ioe);
      throw ioe; // throw it back to kill NM
    }
  }

  private void executeCommand(String[] args, Map<String, String> env) throws IOException {
    ShellCommandExecutor shexec = new ShellCommandExecutor(args, null, env);
    LOG.info("Checking for local volume. If volume is not present command will create and mount it. " +
        "Command invoked is : {}", shexec.toString());
    try {
      shexec.execute();
      LOG.info("Successfully created volume and mounted at {}", args[2]);
    } catch (IOException ioe) {
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        LOG.error("Failed to create and mount local volume at {}. Please see logs at {}"
            + LOCAL_VOLUME_CREATE_SCRIPT_LOGFILE_PATH, args[2], MAPR_INSTALL_DIR);
        LOG.error("Command ran {}", shexec.toString());
        LOG.error("Command output {}", shexec.getOutput());
      }
      throw ioe;
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (LOG.isInfoEnabled()) {
      LOG.info("Shutting down Volume Checker service");
    }
    volumeChecker.shutdown();
    if (deletionService != null) {
      LOG.info("Cleanup service shutdown");
      deletionService.shutdown();
      boolean terminated = false;
      try {
        terminated = deletionService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (!terminated) {
        deletionService.shutdownNow();
      }
    }
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    String jobUser = context.getUser();
    ApplicationId appId = context.getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    final String jobIdStr = jobId.toString();

    LOG.debug("In initializeApplication. Application Id: {}, Job Id: {}", appId.getId(), jobIdStr);

    if (jobMetaData.get(jobIdStr) != null) {
      LOG.debug("Fids for job: {} already created. skipping initializeApplication.", jobIdStr);
      return;
    }
    LOG.info("initializeApplication for job: {} and user: {}", jobIdStr, jobUser);

    // Get the group to which this user belongs. If there are multiple groups,
    // choose the first.
    String jobUserGroup = null;
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(jobUser);
    String[] groupNames = ugi.getGroupNames();
    if (groupNames != null && groupNames.length > 0) {
      jobUserGroup = groupNames[0];
    }

    LOG.debug("User: {} Group: {}", jobUser, jobUserGroup);

    readLock.lock();
    try {
      final String[] jobFids = new String[fidRoots.length];
      // use only this after FileClient gets fid,relpath->fid lookup cache
      //
      jobFids[FidId.ROOT.ordinal()] = fidRoots[FidId.ROOT.ordinal()];
      MapRDirectShuffleMetaData data = new MapRDirectShuffleMetaData();
      PathId shuffleRootPathId = this.metaData.getMapReduceDirsPathIds().get(rootDirNames[FidId.ROOT.ordinal()]);

      data.putDirPathId(rootDirNames[FidId.ROOT.ordinal()], shuffleRootPathId);
      data.setNodeManageHostName(LOCALHOSTNAME);
      for (int i = FidId.ROOT.ordinal() + 1; i < fidRoots.length; i++) {
        jobFids[i] = maprfs.mkdirsFid(fidRoots[i], jobIdStr);
        MapRPathId dirPathId = new MapRPathId();
        dirPathId.setFid(jobFids[i]);
        dirPathId.setIps(shuffleRootPathId.getIPs());
        data.putDirPathId(rootDirNames[i], dirPathId);

        LOG.debug("{} fid for {}: {}", FidId.values()[i].name(), jobIdStr, jobFids[i]);

        maprfs.setOwnerFid(jobFids[i], jobUser, jobUserGroup);
      }
      jobMetaData.put(jobIdStr, data);
    } catch (IOException e) {
      LOG.error("Error during initializeApplication. App Id: " + appId.getId() + ", Job Id: " + jobIdStr, e);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext stopAppContext) {
    ApplicationId appId = stopAppContext.getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    final String jobIdStr = jobId.toString();
    if (isShuttingDown) {
      LOG.info("NodeManager is shutting down but {}/{} might still be running. " +
          "Not cleaning up the {} directory in the local volume.", appId.toString(), jobIdStr, jobIdStr);
      return;
    }
    Runnable filesRemoverTask = new Runnable() {
      @Override
      public void run() {
        readLock.lock();
        try {
          for (int i = FidId.ROOT.ordinal() + 1; i < fidRoots.length; i++) {
            final String fidRoot = fidRoots[i];
            final FidId fidId = FidId.values()[i];
            if (maprfs.deleteFid(fidRoot, jobIdStr)) {
              LOG.debug("Deleted " + jobIdStr + " from " + fidId);
            } else {
              LOG.warn("{} was failed to delete from {}. Parent Fid: {}. There will be another attempt after 3 hours.", jobIdStr, fidId, fidRoot);
              deletionService.schedule(new Runnable() {
                @Override
                public void run() {
                  try {
                    maprfs.deleteFid(fidRoot, jobIdStr);
                  } catch (IOException e) {
                    LOG.warn("{} could not be deleted from {}. Parent Fid: {}", jobIdStr, fidId, fidRoot);
                  }
                }
              }, 3, TimeUnit.HOURS);
            }
          }
        } catch (Throwable t) {
          LOG.error("Error during removing localvolume data for Job Id: {}", jobIdStr, t);
        } finally {
          readLock.unlock();
          jobMetaData.remove(jobIdStr);
        }
      }
    };

    deletionService.schedule(filesRemoverTask, 0, TimeUnit.SECONDS);
  }


  @Override
  public ByteBuffer getMetaData() {
    try {
      DataOutputBuffer dob = new DataOutputBuffer();
      this.metaData.write(dob);
      return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    } catch (IOException e) {
      LOG.error("Encountered error while returning metadata.", e);
      return null;
    }
  }

  @Override
  public ByteBuffer getMetaData(ContainerInitializationContext context) {
    if (context == null || context.getContainerId() == null
        || context.getContainerId().getApplicationAttemptId() == null
        || context.getContainerId().getApplicationAttemptId().getApplicationId() == null) {
      LOG.warn("context is null in getMetaData for context. Returning service metadata");
      return getMetaData();
    }
    ApplicationId appId = context.getContainerId().getApplicationAttemptId().getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    final String jobIdStr = jobId.toString();

    MapRDirectShuffleMetaData jobData = jobMetaData.get(jobIdStr);

    if (jobData == null) {
      LOG.debug("Can not find metadata for a job. Returning service metadata");
      return getMetaData();
    }

    try {
      DataOutputBuffer dob = new DataOutputBuffer();
      jobData.write(dob);
      return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    } catch (IOException e) {
      LOG.error("Encountered error while returning metadata.", e);
      return null;
    }
  }


  protected void initMapReduceDirs() throws IOException {
    writeLock.lock();
    try {
      final String shuffleRootFid =
          maprfs.mkdirsFid(new Path(getNodeManagerDirPath()));

      PathId shuffleRootPathId = new MapRPathId();
      if (!(maprfs instanceof AbstractMapRFileSystem)) {
        throw new UnsupportedOperationException("This is not MapRFileSystem implementation, so can not use createFid method");
      }

      final FSDataOutputStream fileId =
          ((AbstractMapRFileSystem) maprfs).createFid(shuffleRootFid, "fidservers", true);
      shuffleRootPathId.setFid(shuffleRootFid);
      shuffleRootPathId.setIps(fileId.getFidServers());
      fileId.close();

      final String outputDirName = rootDirNames[FidId.OUTPUT.ordinal()];
      final String outputUDirName = rootDirNames[FidId.OUTPUT_U.ordinal()];
      final String spillDirName = rootDirNames[FidId.SPILL.ordinal()];
      final String spillUDirName = rootDirNames[FidId.SPILL_U.ordinal()];

      final PathId oPid = createDirAndGetPathId(shuffleRootPathId, outputDirName);
      final PathId ouPid = createDirAndGetPathId(shuffleRootPathId, outputUDirName);
      final PathId sPid = createDirAndGetPathId(shuffleRootPathId, spillDirName);
      final PathId suPid = createDirAndGetPathId(shuffleRootPathId, spillUDirName);

      // set all the class data only after all createFid/mkdirsFid operations were successfull

      this.metaData.putDirPathId(".", shuffleRootPathId);
      LOG.info("root fid : " + shuffleRootPathId.getFid());

      fidRoots = new String[5];
      fidRoots[FidId.ROOT.ordinal()] = shuffleRootFid;
      this.metaData.putDirPathId(outputDirName, oPid);
      fidRoots[FidId.OUTPUT.ordinal()] = oPid.getFid();
      this.metaData.putDirPathId(outputUDirName, ouPid);
      fidRoots[FidId.OUTPUT_U.ordinal()] = ouPid.getFid();
      this.metaData.putDirPathId(spillDirName, sPid);
      fidRoots[FidId.SPILL.ordinal()] = sPid.getFid();
      this.metaData.putDirPathId(spillUDirName, suPid);
      fidRoots[FidId.SPILL_U.ordinal()] = suPid.getFid();

    } finally {
      writeLock.unlock();
    }
  }


  private PathId createDirAndGetPathId(
      PathId parentPathId, String dirName) throws IOException {
    String dirFid = maprfs.mkdirsFid(parentPathId.getFid(), dirName);
    PathId dirPathId = new MapRPathId();
    dirPathId.setFid(dirFid);
    dirPathId.setIps(parentPathId.getIPs());

    LOG.info(dirName + " fid : " + dirFid);
    return dirPathId;
  }

  /**
   * constructs the mapreduce volume mount path.
   * for e.g. "/var/mapr/local/<hostname>/mapred/"
   *
   * @return MapRedLocalVolumeMountPath
   */
  @VisibleForTesting
  String getMapRedLocalVolumeMountPath() {
    return this.conf.get(MAPR_MAPRED_LOCAL_VOLUME_PATH, "/var/mapr/local/" + LOCALHOSTNAME + "/mapred");
  }

  /**
   * constructs the path for the nodemanager directory inside the local volume.
   * for e.g. "/var/mapr/local/<hostname>/mapred/nodeManager"
   *
   * @return NodeManagerDirPath
   */
  @VisibleForTesting
  String getNodeManagerDirPath() {
    return this.conf.get(MAPR_NM_LOCAL_VOLUME_PATH, getMapRedLocalVolumeMountPath() + "/nodeManager");
  }

  @VisibleForTesting
  String getStagingLocalVolumeMountPath() {
    return this.conf.get("mapr.staging.localvolume.mount.path", "/var/mapr/local/" + LOCALHOSTNAME + "/nm-staging");
  }

  /**
   * constructs the mapreduce volume mount path.
   * for e.g. "/var/mapr/local/<hostname>/spark/"
   */
  @VisibleForTesting
  String getSparkLocalVolumeMountPath() {
    return this.conf.get("mapr.spark.localvolume.mount.path", "/var/mapr/local/" + LOCALHOSTNAME + "/spark");
  }

  private String getSpillDirName() {
    return conf.get(MAPR_LOCALSPILL_DIR_PARAM, MAPR_LOCALSPILL_DIR_DEFAULT);
  }
}
