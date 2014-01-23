/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.Properties;
import java.util.Enumeration;
import java.lang.ClassLoader;
import java.net.URL;
import java.io.DataOutputStream;
import java.text.ParseException;
import java.lang.Math;

import javax.crypto.SecretKey;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.MapRConf;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.mapreduce.server.tasktracker.*;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.*;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.mapreduce.util.MemoryCalculatorPlugin;
import org.apache.hadoop.mapreduce.util.ResourceCalculatorPlugin;
import org.apache.hadoop.mapreduce.util.ProcfsBasedProcessTree;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.MRAsyncDiskService;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.mapreduce.util.CentralConfigUpdaterThread;
import org.apache.hadoop.conf.MapRConf;

import com.mapr.security.ClusterServerTicketGeneration;

import net.java.dev.eval.Expression;
import java.math.BigDecimal;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
public class TaskTracker implements MRConstants, TaskUmbilicalProtocol,
    Runnable, TaskTrackerMXBean {
  /**
   * @deprecated
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY =
    "mapred.tasktracker.vmem.reserved";
  /**
   * @deprecated  TODO(todd) this and above are removed in YDist
   */
  @Deprecated
  static final String MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY =
     "mapred.tasktracker.pmem.reserved";

  public static final String TT_RESERVED_PHYSCIALMEMORY_MB =
    "mapreduce.tasktracker.reserved.physicalmemory.mb";
  
  public static final String TT_RESERVED_PHYSCIALMEMORY_MB_LOW =
    "mapreduce.tasktracker.reserved.physicalmemory.mb.low";
  
  public static final String TT_HEAPBASED_MEMORY_MGMT  =
    "mapreduce.tasktracker.heapbased.memory.management";

  static final String CONF_VERSION_KEY = "mapreduce.tasktracker.conf.version";
  static final String CONF_VERSION_DEFAULT = "default";

  static final String TT_FID_PROPERTY = "mapr.job.fids";

  public static final String TT_VOLUME_SCRIPT_PATH="/server/createTTVolume.sh";
  public static final String TT_VOLUME_LOGFILE_PATH="/logs/createTTVolume.log";
 
  /** MapR's internal diagnostics script. 
   *  User specified debug script is ran before running this script.
   */ 
  public static final String TIMEDOUT_TASK_DIAGNOSTIC_SCRIPT = "/server/collectTaskDiagnostics.sh";
  public static final String TASK_DIAGNOSTICS_DIR = "/diagnostics/";
  /* MapR: Send TT slot stats to hoststats */
  private static final String TT_STATS_FILENAME = "/logs/TaskTracker.stats";
  private static final String MAPRCLI = "/bin/maprcli";
  private static final String SPACE_ALARM_NAME = "NODE_ALARM_TT_LOCALDIR_FULL";
  private static final long ALARM_TIMEOUT = 10000L;
  private static final long ONE_MB = 1024L * 1024L;
  private static final String TASK_CONTROLLER_CONFIG_FILE="/conf/taskcontroller.cfg";
  public static final String HOSTNAME_FILE = "/hostname";
  static final String MAPR_INSTALL_DIR = MapRConf.getPathToMaprHome();

  // set hostname file
  public static final String hostNameFile = MAPR_INSTALL_DIR + HOSTNAME_FILE;

  static final long WAIT_FOR_DONE = 3 * 1000;
  private int httpPort;

  static enum State {NORMAL, STALE, INTERRUPTED, DENIED}

  static final FsPermission LOCAL_DIR_PERMISSION =
        FsPermission.createImmutable((short) 0755);  

  private static final String CENTRAL_CONFIG_URI = 
    "maprfs:///var/mapr/cluster/mapred/mapred-site.xml";
  private static final JobConf DEFAULT_CONF;
  static{
    Configuration.addDefaultResource("mapred-default.xml");
    /* Bug 3595: Disable central config 
    try {
      Configuration.addRemoteResource(
        new URI(CENTRAL_CONFIG_URI), false);
    } catch (Exception e) {}
    */
    Configuration.addDefaultResource("mapred-site.xml");
    DEFAULT_CONF = new JobConf();
    initializeHostname();
  }

  public static final Log LOG =
    LogFactory.getLog(TaskTracker.class);

  public static final String MR_CLIENTTRACE_FORMAT =
        "src: %s" +     // src IP
        ", dest: %s" +  // dst IP
        ", bytes: %s" + // byte count
        ", op: %s" +    // operation
        ", cliID: %s" +  // task id
        ", duration: %s"; // duration
  public static final Log ClientTraceLog =
    LogFactory.getLog(TaskTracker.class.getName() + ".clienttrace");

  //Job ACLs file is created by TaskController under userlogs/$jobid directory
  //for each job at job localization time. This will be used by TaskLogServlet 
  //for authorizing viewing of task logs of that job
  static String jobACLsFile = "job-acls.xml";

  volatile boolean running = true;
  long lastHeartbeat = 0;
  long lastStatUpdate = 0;
  boolean slotsChanged = false;

  private LocalDirAllocator localDirAllocator;
  private String[] localdirs;
  String taskTrackerName;
  static String LOCALHOSTNAME;
  String pidDir = null;
  public static final String RESOURCES_FILE = "/logs/cpu_mem_disk";
  String resourcesFile = null;
  private MapRFsOutputFile maprTTLayout = null;
    
  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  InterTrackerProtocol jobClient;
  
  private TrackerDistributedCacheManager distributedCacheManager;
  static int FILE_CACHE_SIZE = 2000;
    
  // last heartbeat response recieved
  short heartbeatResponseId = -1;
  
  static final String TASK_CLEANUP_SUFFIX = ".cleanup";

  /*
   * This is the last 'status' report sent by this tracker to the JobTracker.
   * 
   * If the rpc call succeeds, this 'status' is cleared-out by this tracker;
   * indicating that a 'fresh' status report be generated; in the event the
   * rpc calls fails for whatever reason, the previous status report is sent
   * again.
   */
  TaskTrackerStatus status = null;

  // The system-directory on HDFS where job files are stored 
  Path systemDirectory = null;
  
  // The filesystem where job files are stored
  FileSystem systemFS = null;
  private LocalFileSystem localFs = null;
  private final HttpServer server;
    
  volatile boolean shuttingDown = false;
    
  Map<TaskAttemptID, TaskInProgress> tasks = new HashMap<TaskAttemptID, TaskInProgress>();
  /**
   * Map from taskId -> TaskInProgress.
   */
  Map<TaskAttemptID, TaskInProgress> runningTasks = null;
  Map<JobID, RunningJob> runningJobs = new TreeMap<JobID, RunningJob>();
  private final JobTokenSecretManager jobTokenSecretManager
    = new JobTokenSecretManager();

  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }

  RunningJob getRunningJob(JobID jobId) {
    return runningJobs.get(jobId);
  }

  volatile int mapTotal = 0;
  volatile int reduceTotal = 0;
  boolean justStarted = true;
  boolean justInited = true;
  boolean isTmpFs = false;
  // Mark reduce tasks that are shuffling to rollback their events index
  Set<TaskAttemptID> shouldReset = new HashSet<TaskAttemptID>();
    
  //dir -> DF
  Map<String, DF> localDirsDf = new HashMap<String, DF>();
  long minSpaceStart = 0L;
  long minSpaceLeft = 0L;
  Boolean isAlarmRaised = null;
  //must have this much space free to start new tasks
  boolean acceptNewTasks = true;
  long minSpaceKill = 0L;
  //if we run under this limit, kill one task
  //and make sure we never receive any new jobs
  //until all the old tasks have been cleaned up.
  //this is if a machine is so full it's only good
  //for serving map output to the other nodes

  static Random r = new Random();
  public static final String SUBDIR = "taskTracker";
  static final String DISTCACHEDIR = "distcache";
  static final String JOBCACHE = "jobcache";
  static final String OUTPUT = "output";
  static final String JARSDIR = "jars";
  static final String LOCAL_SPLIT_FILE = "split.info";
  static final String JOBFILE = "job.xml";
  static final String TT_PRIVATE_DIR = "ttprivate";
  public static final String TT_LOG_TMP_DIR = "tt_log_tmp";
  static final String JVM_EXTRA_ENV_FILE = "jvm.extra.env";

  static final String JOB_LOCAL_DIR = "job.local.dir";
  static final String JOB_TOKEN_FILE="jobToken"; //localized file
  static final String USER_TICKET_FILE="ticketfile"; //localized file

  private JobConf fConf;
  private JobConf originalConf;
  private Localizer localizer;
  private int maxMapSlots;
  private int maxReduceSlots;
  private int maxEphemeralSlots = 0;
  public static final int SMALLJOB_MAX_TASK_MEMORY = 200; // 200mb
  public static final long SMALLJOB_MAX_TASK_ULIMIT = (1L << 32); // 4gb
  public static final long SMALLJOB_TASK_TIMEOUT = 10*1000L; //10 sec
  private long ephemeralSlotsTimeout = SMALLJOB_TASK_TIMEOUT;
  private long ephemeralSlotsUlimit = SMALLJOB_MAX_TASK_ULIMIT;
  private int ephemeralSlotsMemlimit = SMALLJOB_MAX_TASK_MEMORY;
  private int failures;
  final long mapRetainSize;
  final long reduceRetainSize;

  private ACLsManager aclsManager;
  
  // Performance-related config knob to send an out-of-band heartbeat
  // on task completion
  static final String TT_OUTOFBAND_HEARBEAT =
    "mapreduce.tasktracker.outofband.heartbeat";
  private volatile boolean oobHeartbeatOnTaskCompletion;

  static final String TT_JVM_IDLE_TIME = "mapreduce.tasktracker.jvm.idle.time";
  long maxJvmIdleTime = 10*1000; // 10 sec

  static final String TT_VOLUME_HEALTHCHECK_INTERVAL =
    "mapreduce.tasktracker.volume.healthcheck.interval";
  long lastVolumeHealthCheck = 0;
  int ttVolumeCheckInterval = 60*1000; // a minute
  static final String TT_WAIT_BEFORE_TASK_LAUNCH = 
    "mapreduce.tasktracker.task.slowlaunch";
  boolean waitAfterTaskLaunch = false;
  
  // Track number of completed tasks to send an out-of-band heartbeat
  private IntWritable finishedCount = new IntWritable(0);
  private IntWritable currentMemoryUsed = new IntWritable(0);
  private boolean launcherWaiting = false;
  private int defaultMapTaskMemory = -1, defaultReduceTaskMemory = -1;
  private boolean maxHeapSizeBasedChecking = false;
  
  // Thread that updates central config on this node
  private CentralConfigUpdaterThread centralConfigUpdater = null;
  private MapEventsFetcherThread mapEventsFetcher;
  final int workerThreads;
  CleanupQueue directoryCleanupThread;
  private volatile JvmManager jvmManager;
  
  private TaskMemoryManagerThread taskMemoryManager;
  private boolean taskMemoryManagerEnabled = false;
  private long totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long reduceSlotSizeMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private long totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
  private long reservedPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
  private ResourceCalculatorPlugin resourceCalculatorPlugin = null;

  private UserLogManager userLogManager;

  static final String MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY =
      "mapred.tasktracker.memory_calculator_plugin";
  static final String TT_RESOURCE_CALCULATOR_PLUGIN =
    "mapreduce.tasktracker.resourcecalculatorplugin";
  // Performance-related config knob to send an out-of-band heartbeat
  // on task completion
  private int maxMapPrefetch = 0;
  static final String TT_PREFETCH_MAPTASKS = "mapreduce.tasktracker.prefetch.maptasks";
  int cpuCount = 0, diskCount = 0;
  int mfsCpuCount = 0, mfsDiskCount = 0;
  float totalMemInGB = 0.0f;
  long reservedMemInMB = 0;
  String ttConfigFile;
  long lastReload = 0; 
  String mapSlotsStr = null, reduceSlotsStr = null;
  private long lastSlotUpdateTime = 0;
  private boolean isRunningWithLowMemory = false;
  private int currentMaxMapSlots, currentMaxReduceSlots;
  int mapTaskMem, reduceTaskMem;

  private MRAsyncDiskService asyncDiskService;
  
  /**
   * the minimum interval between jobtracker polls
   */
  private volatile int heartbeatInterval = HEARTBEAT_INTERVAL_MIN;
  /**
   * Number of maptask completion events locations to poll for at one time
   */  
  private int probe_sample_size = 500;

  private IndexCache indexCache = null;

  /**
  * Handle to the specific instance of the {@link TaskController} class
  */
  private TaskController taskController;
  
  /**
   * Handle to the specific instance of the {@link NodeHealthCheckerService}
   */
  private NodeHealthCheckerService healthChecker;
  
  /*
   * A list of commitTaskActions for whom commit response has been received 
   */
  private List<TaskAttemptID> commitResponses = 
            Collections.synchronizedList(new ArrayList<TaskAttemptID>());

  private ShuffleServerMetrics shuffleServerMetrics;
  /** This class contains the methods that should be used for metrics-reporting
   * the specific metrics for shuffle. The TaskTracker is actually a server for
   * the shuffle and hence the name ShuffleServerMetrics.
   */
  private class ShuffleServerMetrics implements Updater {
    private MetricsRecord shuffleMetricsRecord = null;
    private int serverHandlerBusy = 0;
    private long outputBytes = 0;
    private int failedOutputs = 0;
    private int successOutputs = 0;
    ShuffleServerMetrics(JobConf conf) {
      MetricsContext context = MetricsUtil.getContext("mapred");
      shuffleMetricsRecord = 
                           MetricsUtil.createRecord(context, "shuffleOutput");
      this.shuffleMetricsRecord.setTag("sessionId", conf.getSessionId());
      context.registerUpdater(this);
    }
    synchronized void serverHandlerBusy() {
      ++serverHandlerBusy;
    }
    synchronized void serverHandlerFree() {
      --serverHandlerBusy;
    }
    synchronized void outputBytes(long bytes) {
      outputBytes += bytes;
    }
    synchronized void failedOutput() {
      ++failedOutputs;
    }
    synchronized void successOutput() {
      ++successOutputs;
    }
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        if (workerThreads != 0) {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent", 
              100*((float)serverHandlerBusy/workerThreads));
        } else {
          shuffleMetricsRecord.setMetric("shuffle_handler_busy_percent", 0);
        }
        shuffleMetricsRecord.incrMetric("shuffle_output_bytes", 
                                        outputBytes);
        shuffleMetricsRecord.incrMetric("shuffle_failed_outputs", 
                                        failedOutputs);
        shuffleMetricsRecord.incrMetric("shuffle_success_outputs", 
                                        successOutputs);
        outputBytes = 0;
        failedOutputs = 0;
        successOutputs = 0;
      }
      shuffleMetricsRecord.update();
    }
  }
    
  private TaskTrackerInstrumentation myInstrumentation = null;

  public TaskTrackerInstrumentation getTaskTrackerInstrumentation() {
    return myInstrumentation;
  }
  
  /**
   * A list of tips that should be cleaned up.
   */
  private BlockingQueue<TaskTrackerAction> tasksToCleanup = 
    new LinkedBlockingQueue<TaskTrackerAction>();
    
  /**
   * A daemon-thread that pulls tips off the list of things to cleanup.
   */
  private Thread taskCleanupThread = 
    new Thread(new Runnable() {
        public void run() {
          while (true) {
            try {
              TaskTrackerAction action = tasksToCleanup.take();
              checkJobStatusAndWait(action);
              if (action instanceof KillJobAction) {
                purgeJob((KillJobAction) action);
              } else if (action instanceof KillTaskAction) {
                processKillTaskAction((KillTaskAction) action);
              } else {
                LOG.error("Non-delete action given to cleanup thread: "
                          + action);
              }
            } catch (Throwable except) {
              LOG.warn(StringUtils.stringifyException(except));
            }
          }
        }
      }, "taskCleanup");
	

  void processKillTaskAction(KillTaskAction killAction) throws IOException {
    TaskInProgress tip;
    synchronized (TaskTracker.this) {
      tip = tasks.get(killAction.getTaskID());
    }
    LOG.info("Received KillTaskAction for task: " + killAction.getTaskID());
    purgeTask(tip, false, true);
  }
  
  private void checkJobStatusAndWait(TaskTrackerAction action) 
  throws InterruptedException {
    JobID jobId = null;
    if (action instanceof KillJobAction) {
      jobId = ((KillJobAction)action).getJobID();
    } else if (action instanceof KillTaskAction) {
      jobId = ((KillTaskAction)action).getTaskID().getJobID();
    } else {
      return;
    }
    RunningJob rjob = null;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);
    }
    if (rjob != null) {
      synchronized (rjob) {
        while (rjob.localizing) {
          rjob.wait();
        }
      }
    }
  }

  public TaskController getTaskController() {
    return taskController;
  }
  
  // Currently this is used only by tests
  void setTaskController(TaskController t) {
    taskController = t;
  }
 
  private void increaseMapTask(int delta) {
    mapTotal += delta;
  }
  private void increaseReduceTask(int delta) {
    reduceTotal += delta;
  }
  private void decreaseMapTask(int delta) {
    mapTotal -= delta;
    if (mapTotal < 0) 
      mapTotal = 0;
  }
  private void decreaseReduceTask(int delta) {
    reduceTotal -= delta;
    if (reduceTotal < 0) 
      reduceTotal = 0;
  }

  /* Removes task from running list. Assumes TT is locked by caller */
  private void removeTask(TaskAttemptID task, boolean isMap) {
    if (runningTasks.get(task) != null) {
      if (isMap) {
        decreaseMapTask(1);
      } else {
        decreaseReduceTask(1);
      }
      runningTasks.remove(task);
    }
  }

  private RunningJob addTaskToJob(JobID jobId, 
                                  TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rJob = null;
      if (!runningJobs.containsKey(jobId)) {
        rJob = new RunningJob(jobId);
        rJob.tasks = new HashSet<TaskInProgress>();
        runningJobs.put(jobId, rJob);
      } else {
        rJob = runningJobs.get(jobId);
      }
      synchronized (rJob) {
        rJob.tasks.add(tip);
      }
      return rJob;
    }
  }

  private void removeTaskFromJob(JobID jobId, TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rjob = runningJobs.get(jobId);
      if (rjob == null) {
        LOG.warn("Unknown job " + jobId + " being deleted.");
      } else {
        synchronized (rjob) {
          rjob.tasks.remove(tip);
        }
      }
    }
  }

  /* Returns path of script to run if task is stuck */
  String getTaskTimeoutDiagnosticsScript() {
    return MAPR_INSTALL_DIR + TIMEDOUT_TASK_DIAGNOSTIC_SCRIPT;
  }

  /* Create a file containing pid in HADOOP_PID_DIR */
  void createPidFile(JVMId jvmId, String pid) {
    if (pidDir != null) {
      try {
        FSDataOutputStream pidFile = 
          localFs.create(new Path(pidDir, jvmId.toString() + ".pid"));
        pidFile.writeBytes(pid+"\n");
        pidFile.close();
      } catch (Exception e) {
        LOG.warn("Error " + e);
      }
    }
  }

  /* Remove a file containing pid in HADOOP_PID_DIR */
  void removePidFile(JVMId jvmId) {
    if (pidDir != null) {
      try {
        localFs.delete(new Path(pidDir, jvmId.toString() + ".pid"));
      } catch (Exception e) {
        LOG.warn("Error " + e);
      }
    }
  }
 
  public long getEphemeralSlotUlimit() {
    return ephemeralSlotsUlimit;
  }
  
  public long getEphemeralSlotMemlimit() {
    return ephemeralSlotsMemlimit;
  }

  /* Called by jsp */
  public JobConf getConf() {
    return this.fConf;
  }

  /* Called by jsp */
  public long getHeapUsed() {
    return Runtime.getRuntime().totalMemory();
  }

  /* Called by jsp */
  public long getHeapSize() {
    return Runtime.getRuntime().maxMemory();
  }

  /* Called by jsp */
  public long getTaskReservedMemory() {
    return reservedMemInMB;
  }

  /* Called by jsp */
  public float getMachineMemory() {
    return totalMemInGB;
  }
  
  /* If /tmp is tmpfs or ramfs then enable hotspot instrumentation */
  public boolean shouldUsePerfData() {
    return isTmpFs;
  }

  public static boolean useMapRFs(Configuration conf) {
    return !("local".equals(conf.get("mapred.job.tracker")))
        && conf.getBoolean("mapreduce.use.maprfs", false);
  }
  
  UserLogManager getUserLogManager() {
    return this.userLogManager;
  }

  void setUserLogManager(UserLogManager u) {
    this.userLogManager = u;
  }

  public static String getUserDir(String user) {
    return TaskTracker.SUBDIR + Path.SEPARATOR + user;
  } 

  Localizer getLocalizer() {
    return localizer;
  }

  void setLocalizer(Localizer l) {
    localizer = l;
  }

  public static String getPrivateDistributedCacheDir(String user) {
    return getUserDir(user) + Path.SEPARATOR + TaskTracker.DISTCACHEDIR;
  }
  
  public static String getPublicDistributedCacheDir() {
    return TaskTracker.SUBDIR + Path.SEPARATOR + TaskTracker.DISTCACHEDIR;
  }

  public static String getJobCacheSubdir(String user) {
    return getUserDir(user) + Path.SEPARATOR + TaskTracker.JOBCACHE;
  }

  public static String getLocalJobDir(String user, String jobid) {
    return getJobCacheSubdir(user) + Path.SEPARATOR + jobid;
  }

  static String getLocalJobConfFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JOBFILE;
  }

  static String getPrivateDirJobConfFile(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + getLocalJobConfFile(user, jobid);
  }

  static String getTaskConfFile(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    return getLocalTaskDir(user, jobid, taskid, isCleanupAttempt)
    + Path.SEPARATOR + TaskTracker.JOBFILE;
  }
  
  static String getPrivateDirTaskScriptLocation(String user, String jobid, 
     String taskid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + 
           getLocalTaskDir(user, jobid, taskid);
  }

  static String getJobJarsDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JARSDIR;
  }

  public static String getJobJarFile(String user, String jobid) {
    return getJobJarsDir(user, jobid) + Path.SEPARATOR + "job.jar";
  }
  
  static String getJobWorkDir(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + MRConstants.WORKDIR;
  }

  static String getLocalSplitFile(String user, String jobid, String taskid) {
    return TaskTracker.getLocalTaskDir(user, jobid, taskid) + Path.SEPARATOR
    + TaskTracker.LOCAL_SPLIT_FILE;
  }

  static String getIntermediateOutputDir(String user, String jobid,
      String taskid) {
    return getLocalTaskDir(user, jobid, taskid) + Path.SEPARATOR
    + TaskTracker.OUTPUT;
  }

  public static String getLocalTaskDir(String user, String jobid, 
      String taskid) {
    return getLocalTaskDir(user, jobid, taskid, false);
  }
  
  public static String getLocalTaskDir(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    String taskDir = getLocalJobDir(user, jobid) + Path.SEPARATOR + taskid;
    if (isCleanupAttempt) {
      taskDir = taskDir + TASK_CLEANUP_SUFFIX;
    }
    return taskDir;
  }
  
  static String getTaskWorkDir(String user, String jobid, String taskid,
      boolean isCleanupAttempt) {
    String dir = getLocalTaskDir(user, jobid, taskid, isCleanupAttempt);
    return dir + Path.SEPARATOR + MRConstants.WORKDIR;
  }

  static String getLocalJobTokenFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.JOB_TOKEN_FILE;
  }

  static String getLocalUserTicketJobFile(String user, String jobid) {
    return getLocalJobDir(user, jobid) + Path.SEPARATOR + TaskTracker.USER_TICKET_FILE;
  }

  static String getPrivateDirUserTicketFile(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + 
    getLocalUserTicketJobFile(user, jobid); 
  }

  static String getPrivateDirJobTokenFile(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + 
           getLocalJobTokenFile(user, jobid); 
  }
  
  static String getPrivateDirForJob(String user, String jobid) {
    return TT_PRIVATE_DIR + Path.SEPARATOR + getLocalJobDir(user, jobid) ;
  }

  private FileSystem getFS(final Path filePath, JobID jobId,
      final Configuration conf) throws IOException, InterruptedException {
    RunningJob rJob = runningJobs.get(jobId);
    FileSystem userFs = 
      rJob.ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return filePath.getFileSystem(conf);
      }});
    return userFs;
  }
  
  String getPid(TaskAttemptID tid) {
    TaskInProgress tip = tasks.get(tid);
    if (tip != null) {
      return jvmManager.getPid(tip.getTaskRunner());
    }
    return null;
  }
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(TaskUmbilicalProtocol.class.getName())) {
      return TaskUmbilicalProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol for TaskTracker: " +
                            protocol);
    }
  }

  /**
   * Delete all of the user directories.
   * @param conf the TT configuration
   * @throws IOException
   */
  private void deleteUserDirectories(Configuration conf) throws IOException {
    for(String root: localdirs) {
      for(FileStatus status: localFs.listStatus(new Path(root, SUBDIR))) {
        boolean deleted = false;
        Throwable t = null;
        // try to delete within TT 
        //
        try {
          deleted = localFs.delete(status.getPath(), true);
        } catch (IOException ioe) {
          t = ioe;
        }

        if (!deleted) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Exception while deleting user directory within " +
                     "TaskTracker. Directory = " + status.getPath() +
                     ". Retrying using TaskController", t);
          }

          final String owner = status.getOwner();
          final String path = status.getPath().getName();
          if (path.equals(owner)) {
            taskController.deleteAsUser(owner, "");
          }
        }
      }
    }
  }

  private void checkSecurityRequirements() throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    if (!NativeIO.isAvailable()) {
      throw new IOException("Secure IO is necessary to run a secure TaskTracker.");
    }
  }

  void initializeDirectories() throws IOException {
    checkLocalDirs(localFs, localdirs = this.fConf.getLocalDirs(), true);
    deleteUserDirectories(fConf);
    asyncDiskService = new MRAsyncDiskService(fConf);
    asyncDiskService.cleanupAllVolumes();

    final FsPermission ttdir = FsPermission.createImmutable((short) 0755);
    for (String s : localdirs) {
      localFs.mkdirs(new Path(s, SUBDIR), ttdir);
    }
    fConf.deleteLocalFiles(TT_PRIVATE_DIR);
    final FsPermission priv = FsPermission.createImmutable((short) 0700);
    for (String s : localdirs) {
      localFs.mkdirs(new Path(s, TT_PRIVATE_DIR), priv);
    }
    fConf.deleteLocalFiles(TT_LOG_TMP_DIR);
    final FsPermission pub = FsPermission.createImmutable((short) 0755);
    for (String s : localdirs) {
      localFs.mkdirs(new Path(s, TT_LOG_TMP_DIR), pub);
    }

    // Set up the user log directory
    File taskLog = TaskLog.getUserLogDir();
    if (!taskLog.isDirectory() && !taskLog.mkdirs()) {
      LOG.warn("Unable to create taskLog directory : " + taskLog.getPath());
    } else {
      Path taskLogDir = new Path(taskLog.getCanonicalPath());
      try {
        localFs.setPermission(taskLogDir,
                              new FsPermission((short)0755));
      } catch (IOException ioe) {
        throw new IOException(
          "Unable to set permissions on task log directory. " +
          taskLogDir + " should be owned by " +
          "and accessible by user '" + System.getProperty("user.name") +
          "'.", ioe);
      }
    }
    DiskChecker.checkDir(TaskLog.getUserLogDir());
  }

  public static final String TT_USER_NAME = "mapreduce.tasktracker.kerberos.principal";
  public static final String TT_KEYTAB_FILE =
    "mapreduce.tasktracker.keytab.file";  
  /**
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  synchronized void initialize() throws IOException, InterruptedException {
    this.fConf = new JobConf(originalConf);
    /* Setup MapRFs layout for job/task dirs */
    if (TaskTracker.useMapRFs(fConf)) {
      maprTTLayout = new MapRFsOutputFile();
      maprTTLayout.setConf(fConf);
      createTTVolume();
      maprTTLayout.initShuffleVolumeFid();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Starting tasktracker with owner as "
          + getMROwner().getShortUserName());
    }

    // oob h/b and prefetch
    oobHeartbeatOnTaskCompletion = 
      fConf.getBoolean(TT_OUTOFBAND_HEARBEAT, false);

    maxJvmIdleTime = this.fConf.getInt(TT_JVM_IDLE_TIME, 10*1000);
    ttVolumeCheckInterval = this.fConf.getInt(TT_VOLUME_HEALTHCHECK_INTERVAL,
                                              60*1000);
    // Check local disk, start async disk service, and clean up all 
    // local directories.
    initializeDirectories();

    // Check security requirements are met
    checkSecurityRequirements();

    // Clear out state tables
    this.tasks.clear();
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.runningJobs = new TreeMap<JobID, RunningJob>();
    this.mapTotal = 0;
    this.reduceTotal = 0;
    this.acceptNewTasks = true;
    this.status = null;
    // set current memory usage
    currentMemoryUsed.set(0);
    // set task finished
    finishedCount.set(0);
    // set heartbeat variables
    lastHeartbeat = 0;
    heartbeatInterval = HEARTBEAT_INTERVAL_MIN;
    lastStatUpdate = 0;
    lastVolumeHealthCheck = 0;

    this.minSpaceStart = this.fConf.getLong("mapred.local.dir.minspacestart", 0L);
    this.minSpaceLeft = this.fConf.getLong("mapred.local.dir.minspaceleft", 1024L * ONE_MB); // 1GB by default
    if ( this.minSpaceLeft < this.minSpaceStart ) {
    	this.minSpaceLeft = this.minSpaceStart;
    	LOG.warn("Minimum Space Left to schedule tasks should be >= Minimum Space required to Start. " +
    			"Setting: mapred.local.dir.minspaceleft to " + this.minSpaceLeft);
    }
    this.minSpaceKill = this.fConf.getLong("mapred.local.dir.minspacekill", 0L);
    //tweak the probe sample size (make it a function of numCopiers)
    probe_sample_size = this.fConf.getInt("mapred.tasktracker.events.batchsize", 500);
    
    try {
      Class<? extends TaskTrackerInstrumentation> metricsInst = getInstrumentationClass(fConf);
      java.lang.reflect.Constructor<? extends TaskTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {TaskTracker.class} );
      this.myInstrumentation = c.newInstance(this);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by 
      //falling back on the default.
      LOG.error(
        "Failed to initialize taskTracker metrics. Falling back to default.",
        e);
      this.myInstrumentation = new TaskTrackerMetricsInst(this);
    }
    
    // bind address
	String address = fConf.get("mapred.task.tracker.report.address");
	if (address.split(":",2).length != 2) {
		throw new IllegalArgumentException("Invalid address/port: " + address);
	}

    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();
    
    this.jvmManager = new JvmManager(this);
    
    // RPC initialization
    int max = maxMapSlots > maxReduceSlots ? maxMapSlots : maxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    this.taskReportServer = RPC.getServer(this, bindAddress,
        tmpPort, 2 * max, false, this.fConf, this.jobTokenSecretManager);

    // Set service-level authorization security policy
    if (this.fConf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            this.fConf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                MapReducePolicyProvider.class, PolicyProvider.class), 
            this.fConf));
      this.taskReportServer.refreshServiceAcl(fConf, policyProvider);
    }

    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set("mapred.task.tracker.report.address",
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + LOCALHOSTNAME + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);
    /* MapR BUG 1492 Disable central config
    startCentralConfigUpdater();
    */    
    // Initialize DistributedCache and
    // clear out temporary files that might be lying around
    this.distributedCacheManager = 
        new TrackerDistributedCacheManager(this.fConf, taskController, asyncDiskService);
    this.distributedCacheManager.purgeCache();

    /* AH 
    this.jobClient = (InterTrackerProtocol) 
    UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        return RPC.waitForProxy(InterTrackerProtocol.class,
            InterTrackerProtocol.versionID,
            jobTrackAddr, fConf);
      }
    });
    */
    /* Remove ZK:
    String zkString = JobTracker.getZKString(this.fConf);
    if (zkString != null) {
      this.jobClient = (InterTrackerProtocol)
        RPC.getProxy(InterTrackerProtocol.class,
        InterTrackerProtocol.versionID,
        zkString, this.fConf);
    } else {
      this.jobClient = (InterTrackerProtocol)
        RPC.getProxy(InterTrackerProtocol.class,
        InterTrackerProtocol.versionID,
        JobTracker.getAddresses(this.fConf), this.fConf);
    }
    */
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    this.jobClient = (InterTrackerProtocol) 
    ugi.doAs(
        new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        return RPC.getProxy(InterTrackerProtocol.class,
            InterTrackerProtocol.versionID,
            FileSystem.get(fConf), fConf);
      }
    });

    this.justInited = true;
    this.running = true;    
    // start the thread that will fetch map task completion events
    this.mapEventsFetcher = new MapEventsFetcherThread();
    mapEventsFetcher.setDaemon(true);
    mapEventsFetcher.setName(
                             "Map-events fetcher for all reduce tasks " + "on " + 
                             taskTrackerName);
    mapEventsFetcher.start();

    Class<? extends ResourceCalculatorPlugin> clazz =
      fConf.getClass(TT_RESOURCE_CALCULATOR_PLUGIN,
          null, ResourceCalculatorPlugin.class);
    resourceCalculatorPlugin = ResourceCalculatorPlugin
      .getResourceCalculatorPlugin(clazz, fConf);
    LOG.info(" Using ResourceCalculatorPlugin : " + resourceCalculatorPlugin);

    // MapR bug 3711 Check if Xmx based checking is enabled
    this.maxHeapSizeBasedChecking = fConf.getBoolean(TT_HEAPBASED_MEMORY_MGMT, false);
    if (!maxHeapSizeBasedChecking) {
      initializeMemoryManagement();
    } else {
      LOG.info("Max HeapSize (-Xmx) based memory management is enabled.");
    }
    getUserLogManager().clearOldUserLogs(fConf);
    
    // no need of index cache for mapr
    if (!TaskTracker.useMapRFs(fConf))
      setIndexCache(new IndexCache(this.fConf));
    
    waitAfterTaskLaunch = fConf.getBoolean(TT_WAIT_BEFORE_TASK_LAUNCH, false);
    mapLauncher = new TaskLauncher(TaskType.MAP, maxMapSlots);
    reduceLauncher = new TaskLauncher(TaskType.REDUCE, maxReduceSlots);
    ephemeralTaskLauncher = new TaskLauncher(TaskType.EPHEMERAL, maxEphemeralSlots);
    mapLauncher.start();
    reduceLauncher.start();
    ephemeralTaskLauncher.start();

    // create a localizer instance
    setLocalizer(new Localizer(localFs, fConf.getLocalDirs()));

    //Start up node health checker service.
    if (shouldStartHealthMonitor(this.fConf)) {
      startHealthMonitor(this.fConf);
    }
    // First time write tasktracker stats
    writeTaskTrackerStats();
  }

  UserGroupInformation getMROwner() {
    return aclsManager.getMROwner();
  }

  private static synchronized void initializeHostname() {
    // Get hostname. If its set in conf file use it
    LOCALHOSTNAME = DEFAULT_CONF.get("slave.host.name");
    if (LOCALHOSTNAME == null) {
      LOCALHOSTNAME = getMapRHostname();
    }
    if (LOCALHOSTNAME == null) {
      try {
        LOCALHOSTNAME = DNS.getDefaultHost(
          DEFAULT_CONF.get("mapred.tasktracker.dns.interface", "default"),
          DEFAULT_CONF.get("mapred.tasktracker.dns.nameserver", "default"));
      } catch (IOException ioe) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Failed to retrieve local host name", ioe);
        }
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Are ACLs for authorization checks enabled on the TT ?
   */
  boolean areACLsEnabled() {
    return fConf.getBoolean(JobConf.MR_ACLS_ENABLED, false);
  }

  public static Class<? extends TaskTrackerInstrumentation> getInstrumentationClass(
    Configuration conf) {
    return conf.getClass("mapred.tasktracker.instrumentation",
        TaskTrackerMetricsInst.class, TaskTrackerInstrumentation.class);
  }

  public static void setInstrumentationClass(
    Configuration conf, Class<? extends TaskTrackerInstrumentation> t) {
    conf.setClass("mapred.tasktracker.instrumentation",
        t, TaskTrackerInstrumentation.class);
  }
  
  /** 
   * Removes all contents of temporary storage.  Called upon 
   * startup, to remove any leftovers from previous run.
   *
   * Use MRAsyncDiskService.moveAndDeleteAllVolumes instead.
   * @see org.apache.hadoop.mapreduce.util.MRAsyncDiskService#cleanupAllVolumes()
   */
  @Deprecated
  public void cleanupStorage() throws IOException {
    this.fConf.deleteLocalFiles();
  }

  // Object on wait which MapEventsFetcherThread is going to wait.
  private Object waitingOn = new Object();

  private class MapEventsFetcherThread extends Thread {

    private List <FetchStatus> reducesInShuffle() {
      List <FetchStatus> fList = new ArrayList<FetchStatus>();
      for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
        RunningJob rjob = item.getValue();
        if (!rjob.localized) {
          continue;
        }
        JobID jobId = item.getKey();
        FetchStatus f;
        synchronized (rjob) {
          f = rjob.getFetchStatus();
          for (TaskInProgress tip : rjob.tasks) {
            Task task = tip.getTask();
            if (!task.isMapTask()) {
              if (((ReduceTask)task).getPhase() == 
                  TaskStatus.Phase.SHUFFLE) {
                if (rjob.getFetchStatus() == null) {
                  //this is a new job; we start fetching its map events
                  f = new FetchStatus(jobId, 
                                      ((ReduceTask)task).getNumMaps());
                  rjob.setFetchStatus(f);
                }
                f = rjob.getFetchStatus();
                fList.add(f);
                break; //no need to check any more tasks belonging to this
              }
            }
          }
        }
      }
      //at this point, we have information about for which of
      //the running jobs do we need to query the jobtracker for map 
      //outputs (actually map events).
      return fList;
    }
      
    @Override
    public void run() {
      LOG.info("Starting thread: " + this.getName());
        
      while (running) {
        try {
          List <FetchStatus> fList = null;
          synchronized (runningJobs) {
            while (((fList = reducesInShuffle()).size()) == 0) {
              try {
                runningJobs.wait();
              } catch (InterruptedException e) {
                if (!running) {
                  LOG.info("Shutting down: " + this.getName());
                  return;
                } else {
                  LOG.warn("Unexpected InterruptedException");
                }
              }
            }
          }
          // now fetch all the map task events for all the reduce tasks
          // possibly belonging to different jobs
          boolean fetchAgain = false; //flag signifying whether we want to fetch
                                      //immediately again.
          for (FetchStatus f : fList) {
            long currentTime = System.currentTimeMillis();
            try {
              //the method below will return true when we have not 
              //fetched all available events yet
              if (f.fetchMapCompletionEvents(currentTime)) {
                fetchAgain = true;
              }
            } catch (Exception e) {
              LOG.warn(
                       "Ignoring exception that fetch for map completion" +
                       " events threw for " + f.jobId + " threw: " +
                       StringUtils.stringifyException(e)); 
            }
            if (!running) {
              break;
            }
          }
          synchronized (waitingOn) {
            try {
              if (!fetchAgain) {
                waitingOn.wait(heartbeatInterval);
              }
            } catch (InterruptedException ie) {
              if (!running) {
                LOG.info("Shutting down: " + this.getName());
                return;
              } else {
                LOG.warn("Unexpected InterruptedException");
              }
            }
          }
        } catch (Exception e) {
          LOG.info("Ignoring exception "  + e.getMessage());
        }
      }
    } 
  }

  private class FetchStatus {
    /** The next event ID that we will start querying the JobTracker from*/
    private IntWritable fromEventId;
    /** This is the cache of map events for a given job */ 
    private List<TaskCompletionEvent> allMapEvents;
    /** What jobid this fetchstatus object is for*/
    private JobID jobId;
    private long lastFetchTime;
    private boolean fetchAgain;
     
    public FetchStatus(JobID jobId, int numMaps) {
      this.fromEventId = new IntWritable(0);
      this.jobId = jobId;
      this.allMapEvents = new ArrayList<TaskCompletionEvent>(numMaps);
    }
      
    /**
     * Reset the events obtained so far.
     */
    public void reset() {
      // Note that the sync is first on fromEventId and then on allMapEvents
      synchronized (fromEventId) {
        synchronized (allMapEvents) {
          fromEventId.set(0); // set the new index for TCE
          allMapEvents.clear();
        }
      }
    }
    
    public TaskCompletionEvent[] getMapEvents(int fromId, int max) {
        
      TaskCompletionEvent[] mapEvents = 
        TaskCompletionEvent.EMPTY_ARRAY;
      boolean notifyFetcher = false; 
      synchronized (allMapEvents) {
        if (allMapEvents.size() > fromId) {
          int actualMax = Math.min(max, (allMapEvents.size() - fromId));
          List <TaskCompletionEvent> eventSublist = 
            allMapEvents.subList(fromId, actualMax + fromId);
          mapEvents = eventSublist.toArray(mapEvents);
        } else {
          // Notify Fetcher thread. 
          notifyFetcher = true;
        }
      }
      if (notifyFetcher) {
        synchronized (waitingOn) {
          waitingOn.notify();
        }
      }
      return mapEvents;
    }
      
    public boolean fetchMapCompletionEvents(long currTime) throws IOException {
      if (!fetchAgain && (currTime - lastFetchTime) < heartbeatInterval) {
        return false;
      }
      int currFromEventId = 0;
      synchronized (fromEventId) {
        currFromEventId = fromEventId.get();
        List <TaskCompletionEvent> recentMapEvents = 
          queryJobTracker(fromEventId, jobId, jobClient);
        synchronized (allMapEvents) {
          allMapEvents.addAll(recentMapEvents);
        }
        lastFetchTime = currTime;
        if (fromEventId.get() - currFromEventId >= probe_sample_size) {
          //return true when we have fetched the full payload, indicating
          //that we should fetch again immediately (there might be more to
          //fetch
          fetchAgain = true;
          return true;
        }
      }
      fetchAgain = false;
      return false;
    }
  }

  private static LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator();

  // intialize the job directory
  RunningJob localizeJob(TaskInProgress tip)
  throws IOException, InterruptedException {
    Task t = tip.getTask();
    JobID jobId = t.getJobID();
    RunningJob rjob = addTaskToJob(jobId, tip);
    InetSocketAddress ttAddr = getTaskTrackerReportAddress();
    try {
      synchronized (rjob) {
        if (!rjob.localized) {
          while (rjob.localizing) {
            rjob.wait();
          }
          if (!rjob.localized) {
            //this thread is localizing the job
            rjob.localizing = true;
          }
        }
      }
      if (!rjob.localized) {
        Path localJobConfPath = null;
        try {
          removeJobPrivateFilesNow(t.getUser(), jobId);
          localJobConfPath = initializeJob(t, rjob, ttAddr);
        } catch (IOException ioe) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Failed to initialize job " + jobId + " for task "
              + tip.getTask().getTaskID() +
              ". Cleaning up job files from TaskTracker's private cache.");
          }
          removeJobPrivateFilesNow(t.getUser(), jobId);
          throw ioe;
        }
        JobConf localJobConf = new JobConf(localJobConfPath);
        //to be doubly sure, overwrite the user in the config with the one the TT
        //thinks it is
        localJobConf.setUser(t.getUser());
        //also reset the #tasks per jvm
        resetNumTasksPerJvm(localJobConf);
        synchronized (rjob) {
          //set the base jobconf path in rjob; all tasks will use
          //this as the base path when they run
          rjob.localizedJobConf = localJobConfPath;
          rjob.jobConf = localJobConf;  
          rjob.keepJobFiles = ((localJobConf.getKeepTaskFilesPattern() != null) ||
                                localJobConf.getKeepFailedTaskFiles());
          rjob.localized = true;
        }
      }
    } finally {
      synchronized (rjob) {
        if (rjob.localizing) {
          rjob.localizing = false;
          rjob.notifyAll();
        }
      }
    }
    synchronized (runningJobs) {
      runningJobs.notify(); //notify the fetcher thread
    }
    return rjob;
  }

  /**
   * Localize the job on this tasktracker. Specifically
   * <ul>
   * <li>Cleanup and create job directories on all disks</li>
   * <li>Download the credentials file</li>
   * <li>Download the job config file job.xml from the FS</li>
   * <li>Create the job work directory and set {@link TaskTracker#JOB_LOCAL_DIR}
   * in the configuration.
   * <li>Download the job jar file job.jar from the FS, unjar it and set jar
   * file in the configuration.</li>
   * <li>Invokes the {@link TaskController} to do the rest of the job 
   * initialization</li>
   * </ul>
   *
   * @param t task whose job has to be localized on this TT
   * @param rjob the {@link RunningJob}
   * @param ttAddr the tasktracker's RPC address
   * @return the path to the job configuration to be used for all the tasks
   *         of this job as a starting point.
   * @throws IOException
   */
  Path initializeJob(final Task t, final RunningJob rjob, 
      final InetSocketAddress ttAddr)
  throws IOException, InterruptedException {
    final JobID jobId = t.getJobID();

    final Path jobFile = new Path(t.getJobFile());
    final String userName = t.getUser();
    final Configuration conf = getJobConf();

    // save local copy of JobToken file
    final String localJobTokenFile = localizeJobTokenFile(t.getUser(), jobId);
    synchronized (rjob) {
      rjob.ugi = UserGroupInformation.createRemoteUser(t.getUser());
      Credentials ts = TokenCache.loadTokens(localJobTokenFile, conf);
      Token<JobTokenIdentifier> jt = TokenCache.getJobToken(ts);
      if (jt != null) { //could be null in the case of some unit tests
        getJobTokenSecretManager().addTokenForJob(jobId.toString(), jt);
      }
      for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
        rjob.ugi.addToken(token);
      }
    }

    FileSystem userFs = getFS(jobFile, jobId, conf);

    // Download the job.xml for this job from the system FS
    final Path localJobFile =
        localizeJobConfFile(new Path(t.getJobFile()), userName, userFs, jobId);

    final JobConf localJobConf = new JobConf(localJobFile);
    final String[] jobRootFids = maprTTLayout.createJobDirFids(
      jobId, t.getUser(), localJobConf.get("group.name"));

    final StringBuilder sb = new StringBuilder();
    if (jobRootFids.length > 0) {
      int i = 0;
      sb.append(jobRootFids[i++]);
      for (; i < jobRootFids.length;) {
        sb.append(',');
        sb.append(jobRootFids[i++]);
      }
      localJobConf.set(TT_FID_PROPERTY, sb.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing job root fids in the localized config: "
          + sb.toString());
      }
    }

    final String localUserTicketJobFile;
    if ( UserGroupInformation.isSecurityEnabled()) {
      localUserTicketJobFile = localizeUserJobTicketFile(t.getUser(), jobId, localJobConf);
      if ( LOG.isInfoEnabled() ) {
        LOG.info("Userticket location is: " + localUserTicketJobFile);
      }
    } else {
      localUserTicketJobFile = null;
    }

    /**
      * Now initialize the job via task-controller to do the rest of the
      * job-init. Do this within a doAs since the public distributed cache 
      * is also set up here.
      * To support potential authenticated HDFS accesses, we need the tokens
      */
    rjob.ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException, InterruptedException {
        try {
          // Setup the public distributed cache
          TaskDistributedCacheManager taskDistributedCacheManager =
            getTrackerDistributedCacheManager()
           .newTaskDistributedCacheManager(jobId, localJobConf);
          rjob.distCacheMgr = taskDistributedCacheManager;
          taskDistributedCacheManager.setupCache(localJobConf,
            TaskTracker.getPublicDistributedCacheDir(),
            TaskTracker.getPrivateDistributedCacheDir(userName));

          // Set some config values
          localJobConf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY,
              getJobConf().get(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
          if (conf.get("slave.host.name") != null) {
            localJobConf.set("slave.host.name", conf.get("slave.host.name"));
          }
          resetNumTasksPerJvm(localJobConf);
          localJobConf.setUser(t.getUser());

          // write back the config (this config will have the updates that the
          // distributed cache manager makes as well)
          JobLocalizer.writeLocalJobFile(localJobFile, localJobConf);
          taskController.initializeJob(t.getUser(), jobId.toString(), 
              new Path(localJobTokenFile), (localUserTicketJobFile != null) ? new Path(localUserTicketJobFile) : null, localJobFile, TaskTracker.this,
              ttAddr);
        } catch (IOException e) {
          LOG.warn("Exception while localization " + 
              StringUtils.stringifyException(e));
          throw e;
        } catch (InterruptedException ie) {
          LOG.warn("Exception while localization " + 
              StringUtils.stringifyException(ie));
          throw ie;
        }
        return null;
      }
    });
    //search for the conf that the initializeJob created
    //need to look up certain configs from this conf, like
    //the distributed cache, profiling, etc. ones
    Path initializedConf = lDirAlloc.getLocalPathToRead(getLocalJobConfFile(
           userName, jobId.toString()), getJobConf());
    return initializedConf;
  }
  
  /** If certain configs are enabled, the jvm-reuse should be disabled
   * @param localJobConf
   */
  static void resetNumTasksPerJvm(JobConf localJobConf) {
    boolean debugEnabled = false;
    if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
      return;
    }
    if (localJobConf.getMapDebugScript() != null || 
        localJobConf.getReduceDebugScript() != null) {
      debugEnabled = true;
    }
    String keepPattern = localJobConf.getKeepTaskFilesPattern();
     
    if (debugEnabled || localJobConf.getProfileEnabled() ||
        keepPattern != null || localJobConf.getKeepFailedTaskFiles()) {
      //disable jvm reuse
      localJobConf.setNumTasksToExecutePerJvm(1);
    }
  }

  // Remove the log dir from the tasklog cleanup thread
  void saveLogDir(JobID jobId, JobConf localJobConf)
      throws IOException {
    // remove it from tasklog cleanup thread first,
    // it might be added there because of tasktracker reinit or restart
    JobStartedEvent jse = new JobStartedEvent(jobId);
    getUserLogManager().addLogEvent(jse);
  }


  /**
   * Download the job configuration file from the FS.
   *
   * @param jobFile the original location of the configuration file
   * @param user the user in question
   * @param userFs the FileSystem created on behalf of the user
   * @param jobId jobid in question
   * @return the local file system path of the downloaded file.
   * @throws IOException
   */
  private Path localizeJobConfFile(Path jobFile, String user, 
      FileSystem userFs, JobID jobId)
  throws IOException {
    // Get sizes of JobFile and JarFile
    // sizes are -1 if they are not present.
    FileStatus status = null;
    long jobFileSize = -1;
    try {
      status = userFs.getFileStatus(jobFile);
      jobFileSize = status.getLen();
    } catch(FileNotFoundException fe) {
      jobFileSize = -1;
    }
    Path localJobFile =
      lDirAlloc.getLocalPathForWrite(getPrivateDirJobConfFile(user,
          jobId.toString()), jobFileSize, fConf);

    // Download job.xml
    userFs.copyToLocalFile(jobFile, localJobFile);
    return localJobFile;
  }

  private void launchTaskForJob(TaskInProgress tip, JobConf jobConf,
                                RunningJob rjob) throws IOException {
     // set Xmx option right here rather than in TaskRunner
    int memReq = getTaskMemoryRequired(jobConf, tip.getTask().isMapTask(),
                                       tip.isEphemeral());
    /* Bug 3711 do not look at Xmx for oversubscribing unless enabled */
    if (maxHeapSizeBasedChecking) {
      // if this task requires memory that is more than 
      // reserved memory for tasks on TT fail it.
      if (memReq > reservedMemInMB) {
        LOG.error("Task " + tip.getTask().getTaskID() + 
                  " exceeds memory limit " + reservedMemInMB + 
                  "mb. Task requires " + memReq + 
                  "mb. Aborting task launch on " + getName());
        throw new IOException("Task " + tip.getTask().getTaskID() + 
                              " exceeds memory limit " + reservedMemInMB + 
                              "mb. Task requires " + memReq + 
                              "mb. Aborting task launch on " + getName());
      }
      if (tip.isEphemeral() && (memReq > getEphemeralSlotsMemlimit())) {
        LOG.error("Ephemeral Task " + tip.getTask().getTaskID() + 
                  " exceeds memory limit " + getEphemeralSlotsMemlimit() + 
                  "mb. Task requires " + memReq + 
                  "mb. Aborting task launch on " + getName());
        throw new IOException("Ephemeral Task " + tip.getTask().getTaskID() + 
                              " exceeds memory limit " + getEphemeralSlotsMemlimit() + 
                              "mb. Task requires " + memReq + 
                              "mb. Aborting task launch on " + getName());
      }
      // if after running this task total mem used > reserved memory
      // then wait for a slot to finish. This will pause launcher thread 
      // which is fine, since task is not moved from assigned to running.
      if (!tip.isEphemeral())
        reserveTaskMemory(memReq, tip.getTask().isMapTask());
    }
    //make sure task starts now, not when it was added.
    tip.updateLastProgressReport();
    if (LOG.isDebugEnabled())
      LOG.debug("Launching " + tip.getTask().getTaskID() + 
                " with memory = " + memReq + "mb");
    synchronized (tip) {
      tip.setMemoryReserved(memReq);
      tip.setJobConf(jobConf);
      tip.setUGI(rjob.ugi);
      tip.launchTask(rjob);
    }
  }
    
  public synchronized void shutdown() throws IOException, InterruptedException {
    shuttingDown = true;
    close();
    if (this.server != null) {
      try {
        LOG.info("Shutting down StatusHttpServer");
        this.server.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down TaskTracker", e);
      }
    }
  }
  /**
   * Close down the TaskTracker and all its components.  We must also shutdown
   * any running tasks or threads, and cleanup disk space.  A new TaskTracker
   * within the same process space might be restarted, so everything must be
   * clean.
   * @throws InterruptedException 
   */
  public synchronized void close() throws IOException, InterruptedException {
    //stop the launchers
    this.mapLauncher.shutdown();
    this.reduceLauncher.shutdown();
    this.ephemeralTaskLauncher.shutdown();
    this.mapLauncher.interrupt();
    this.reduceLauncher.interrupt();
    this.ephemeralTaskLauncher.interrupt();

    //
    // Kill running tasks.  Do this in a 2nd vector, called 'tasksToClose',
    // because calling jobHasFinished() may result in an edit to 'tasks'.
    //
    TreeMap<TaskAttemptID, TaskInProgress> tasksToClose =
      new TreeMap<TaskAttemptID, TaskInProgress>();
    tasksToClose.putAll(tasks);
    for (TaskInProgress tip : tasksToClose.values()) {
      tip.jobHasFinished(false, true);
    }
    
    this.running = false;

    // Clear local storage
    if (asyncDiskService != null) {
      // Clear local storage
      asyncDiskService.cleanupAllVolumes();
      
      // Shutdown all async deletion threads with up to 10 seconds of delay
      asyncDiskService.shutdown();
      try {
        if (!asyncDiskService.awaitTermination(10000)) {
          asyncDiskService.shutdownNow();
          asyncDiskService = null;
        }
      } catch (InterruptedException e) {
        asyncDiskService.shutdownNow();
        asyncDiskService = null;
      }
    }
        
    // Shutdown the fetcher thread
    this.mapEventsFetcher.interrupt();    

    // shutdown central config thread
    if (this.centralConfigUpdater != null) {
      this.centralConfigUpdater.shutdown();
      this.centralConfigUpdater.interrupt();
    }

    jvmManager.stop();
    
    // shutdown RPC connections
    RPC.stopProxy(jobClient);

    // wait for the fetcher thread to exit
    for (boolean done = false; !done; ) {
      try {
        this.mapEventsFetcher.join();
        done = true;
      } catch (InterruptedException e) {
      }
    }
    
    if (taskReportServer != null) {
      taskReportServer.stop();
      taskReportServer = null;
    }
    if (healthChecker != null) {
      //stop node health checker service
      healthChecker.stop();
      healthChecker = null;
    }
  }

  /**
   * For testing
   */
  TaskTracker() {
    server = null;
    workerThreads = 0;
    mapRetainSize = TaskLogsTruncater.DEFAULT_RETAIN_SIZE;
    reduceRetainSize = TaskLogsTruncater.DEFAULT_RETAIN_SIZE;
  }

  void setConf(JobConf conf) {
    fConf = conf;
  }

  /** Generate a config file to be used by taskcontroller 
    * mapred.local.dir=#configured value of mapred.local.dir. It can be a list of comma separated paths.
    * hadoop.log.dir=#configured value of hadoop.log.dir.
    * mapreduce.tasktracker.group=#configured value of mapreduce.tasktracker.group.
    */
  void writeTaskControllerConfig(JobConf conf) {
    String hadoopHomeDir = System.getenv("HADOOP_HOME");
    if (hadoopHomeDir == null) {
      /* Construct a path */
      hadoopHomeDir = MAPR_INSTALL_DIR + "/hadoop/hadoop-0.20.2/";
    }
    try {
      File tcConfigFile = new File(hadoopHomeDir + TASK_CONTROLLER_CONFIG_FILE);
      DataOutputStream out = new DataOutputStream(new FileOutputStream(tcConfigFile, false));
      String groupName = conf.get("mapreduce.tasktracker.group", "root");
      out.writeBytes("mapred.local.dir=" +
        conf.get(JobConf.MAPRED_LOCAL_DIR_PROPERTY) + "\n");
      out.writeBytes("hadoop.log.dir=" + System.getProperty("hadoop.log.dir") + "\n");
      out.writeBytes("mapreduce.tasktracker.group=" + groupName  + "\n");      
      // disable check for min uid
      out.writeBytes("min.user.id=-1\n");
      // write out value for hadoop authentication
      out.writeBytes(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + "=" 
          + conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "simple") + "\n");
      out.close();
    } catch (Exception e) {
      LOG.warn("Error while writing to TaskController config file" + e);
    }
  }

  // stat volume mount point every min
  void checkTTVolumeHealth() throws DiskErrorException  {
    // fConf is not null
    final Path ttVolumePath =
      new Path(maprTTLayout.getMapRFsPath(LOCALHOSTNAME));
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("Checking mapreduce volume..... " + ttVolumePath);
      systemFS.getFileStatus(ttVolumePath);
    } catch (Exception e) {
      LOG.error("TaskTracker failed to get status of mapreduce volume " + 
                ttVolumePath + ". Error " + e);
      throw new DiskErrorException(
          "TaskTracker failed to get status of mapreduce volume " +
          ttVolumePath + ". Error " + e);
    }
  }

  /* MapR write tasktracker slots info in a file */
  void writeTaskTrackerStats() {
    try {
      File ttStatsFile = new File(MAPR_INSTALL_DIR + TT_STATS_FILENAME);
      DataOutputStream out = new DataOutputStream(new FileOutputStream(ttStatsFile, false));
      int mapSlots = maxMapSlots + maxMapPrefetch;
      out.writeBytes(mapSlots + " " + mapTotal + " " +
                     maxReduceSlots + " " + reduceTotal + " " + "\n");
      out.close();
    } catch (Exception e) {
      LOG.warn("Error while writing to TT stats file" + e);
    }
  }

  private synchronized void createTTVolume() throws IOException {
    final String[] args = new String[] {
      MAPR_INSTALL_DIR + TT_VOLUME_SCRIPT_PATH,
      LOCALHOSTNAME,
      // vol mount path
      maprTTLayout.getMapRVolumeMountPoint(LOCALHOSTNAME),
      // full path
      maprTTLayout.getMapRFsPath(LOCALHOSTNAME)
    };
    ShellCommandExecutor shexec = new ShellCommandExecutor(args);
    if (LOG.isInfoEnabled()) 
      LOG.info("Checking for local volume." +
               " If volume is not present command will create and mount it." + 
               " Command invoked is : " + shexec.toString());
    try {
      shexec.execute();
    } catch (IOException ioe) {
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        LOG.error("Failed to create and mount local mapreduce volume at "
                  + args[2] + ". Please see logs at " + MAPR_INSTALL_DIR
                  + TT_VOLUME_LOGFILE_PATH);
        LOG.error("Command ran " + shexec.toString());
        LOG.error("Command output " + shexec.getOutput()); 
      }
      throw ioe;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Sucessfully created volume and mounted at " + args[2]);
    }
  }

  private void raiseSpaceAlarm(String description) {
    alarmManagingHelper(true, description);
  }
  
  private void clearSpaceAlarm(String description) {
    alarmManagingHelper(false, null);
  }

  private void alarmManagingHelper(boolean raise, String description) {
    final String raised;
    if ( raise ) {
      raised = "raise";
    } else {
      raised = "clear";
    }
    String params = 
      "alarm " + raised + " -alarm " + SPACE_ALARM_NAME + " -entity ";
		
    List<String> commandsList = new ArrayList<String>();
    commandsList.add(MAPR_INSTALL_DIR + MAPRCLI);
    commandsList.addAll(Arrays.asList(params.split(" ")));
    commandsList.add(LOCALHOSTNAME);
    if ( description != null ) {
      commandsList.add("-description");
      commandsList.add(description);
    }
		
    ShellCommandExecutor shexec = 
      new ShellCommandExecutor(commandsList.toArray(new String[0]), 
          null, null, ALARM_TIMEOUT);
    if (LOG.isInfoEnabled()) {
      LOG.info(raised + " space alarm. " +
          " Command invoked is : " + shexec.toString());
    }
    try {
      shexec.execute();
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        LOG.error("Command ran " + shexec.toString());
        LOG.error("Command output " + shexec.getOutput()); 
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("Sucessfully " + raised + " space alarm");
        }
        isAlarmRaised = raise;
      }
    } catch (IOException e) {
      LOG.error("IOException while trying to " + raised + " alarm", e);
      // Strange case: Exception raising an alarm.
      // But the command might have succeeded. Clear it
      if (raise) {
        alarmManagingHelper(false, null);
      }
    }
  }

  private  boolean checkTmpFilesystem() throws IOException {
    String[] tmpfsArgs = {"bash","-c","exec df --type=tmpfs /tmp  2>/dev/null"};
    String[] ramfsArgs = {"bash","-c","exec df --type=ramfs /tmp  2>/dev/null"};
    ShellCommandExecutor shexec = null;
    try { 
      shexec = new ShellCommandExecutor(tmpfsArgs);
      shexec.execute();
      int exitCode = shexec.getExitCode();
      if (exitCode == 0) {
        LOG.info("/tmp is tmpfs. " + 
                 "Java Hotspot Instrumentation will be enabled by default");
        return true;
      }
      shexec = new ShellCommandExecutor(ramfsArgs);
      shexec.execute();
      exitCode = shexec.getExitCode();
      if (exitCode == 0) {
        LOG.info("/tmp is ramfs. " + 
                 "Java Hotspot Instrumentation will be enabled by default");
        return true;
      }
    } catch (Exception e) { 
    }
    LOG.info("/tmp is not tmpfs or ramfs. " + 
             "Java Hotspot Instrumentation will be disabled by default");
    return false;
  }

  /**
   * A filter for conf files
   */
  private static final PathFilter JVM_PID_FILTER = new PathFilter() {
    public boolean accept(Path path) {
      return path.getName().matches("jvm.*\\.pid");
    }
  };

  void startCentralConfigUpdater() {
    URI centralConfigFileUri = null;
    try {
      centralConfigFileUri = new URI(CENTRAL_CONFIG_URI);
      if ("maprfs".equals(centralConfigFileUri.getScheme())) {
        Path remoteFilePath = new Path(centralConfigFileUri.getPath());
        /** Since static initialization is already done,
          * getCachedCentralFilePath will return local file path
          */
        centralConfigUpdater = 
          new CentralConfigUpdaterThread(FileSystem.get(originalConf),
              remoteFilePath, 
              Configuration.getCachedCentralFilePath(remoteFilePath));
        centralConfigUpdater.setDaemon(true);
        centralConfigUpdater.setName("Central configuration thread on " 
            + taskTrackerName);
        centralConfigUpdater.start();
      }
    } catch (Exception e) {
      LOG.warn("Failed to start Central configuration thread with URI " +
               CENTRAL_CONFIG_URI + " on " + taskTrackerName + 
               ". Error " + e);
      centralConfigUpdater = null;
    }
  }
  
  /**
   * Start with the local machine name, and the default JobTracker
   */
  public TaskTracker(JobConf conf) throws IOException, InterruptedException {
    originalConf = conf;
    /* AH 
    maxMapSlots = conf.getInt(
                  "mapred.tasktracker.map.tasks.maximum", 2);
    maxReduceSlots = conf.getInt(
                  "mapred.tasktracker.reduce.tasks.maximum", 2);
    */
    SecurityUtil.login(originalConf, TT_KEYTAB_FILE, TT_USER_NAME);

    this.localFs = FileSystem.getLocal(conf);
    this.isTmpFs = checkTmpFilesystem();

    this.pidDir = System.getProperty("hadoop.pid.dir");
    if (this.pidDir != null) {
      if (!localFs.exists(new Path(this.pidDir))) {
        localFs.mkdirs(new Path(pidDir));
      } else {
        // remove all pids with jvm*.pid
        LOG.info("Cleaning up config files from the job history folder");
        FileStatus[] status = localFs.listStatus(new Path(pidDir),  
                                                 JVM_PID_FILTER);
        for (FileStatus s : status) {
          LOG.info("Deleting old jvm pid file " + s.getPath());
          localFs.delete(s.getPath(), false);
        }
      }
    } else {
      LOG.warn("Couldn't get pid dir");
    }

    maxEphemeralSlots = 
      conf.getInt("mapred.tasktracker.ephemeral.tasks.maximum", 1);
    ephemeralSlotsTimeout = 
      conf.getLong("mapred.tasktracker.ephemeral.tasks.timeout", 
                   SMALLJOB_TASK_TIMEOUT);
    ephemeralSlotsUlimit = 
      conf.getLong("mapred.tasktracker.ephemeral.tasks.ulimit", 
                   SMALLJOB_MAX_TASK_ULIMIT);
    ephemeralSlotsMemlimit = 
      conf.getInt("mapred.cluster.ephemeral.tasks.memory.limit.mb", 
                   SMALLJOB_MAX_TASK_MEMORY); 

    // set resources file
    resourcesFile = MAPR_INSTALL_DIR + this.RESOURCES_FILE;

    getResourceInfo(conf);
    try {
      maxMapSlots = calcMapSlots(
          conf.get("mapred.tasktracker.map.tasks.maximum", "-1"));
    } catch (Exception e) {
      LOG.warn("Invalid string for map slots calculation " +
               "String is " +  mapSlotsStr + " err is " + e);
      maxMapSlots = -1;
    }

    try {
      maxReduceSlots = calcReduceSlots(
          conf.get("mapred.tasktracker.reduce.tasks.maximum", "-1"));
    } catch (Exception e) {
      LOG.warn("Invalid string for reduce slots calculation " +
               "String is " +  reduceSlotsStr + " err is " + e);
      maxReduceSlots = -1;
    }

    // If valid map or reduce slots are specified in config files,
    // the following condition does not hold and the number of slots are
    // not calculated based on desired map and reduce task heap sizes.
    // Calculation of heap sizes for map and reduce tasks is then
    // based on the formula in getDefaultMapTaskJvmMem() and
    // getDefaultReduceTaskJvmMem()

    if ((maxMapSlots == -1) && (maxReduceSlots == -1)) {
      
      // First cap the slots based on CPU/MEM/DISKs 
      MapRedSlotUtil.MachineInfo mInfo = 
              new MapRedSlotUtil.MachineInfo(cpuCount, diskCount, 
                              mfsCpuCount, mfsDiskCount, reservedMemInMB);
      MapRedSlotUtil.MapRedSlotInfo mrInfo = MapRedSlotUtil.adjustSlots(conf, mInfo);
      reservedMemInMB = (mrInfo.maxMapSlots * mrInfo.mapTaskMem) + 
                        (mrInfo.maxReduceSlots * mrInfo.reduceTaskMem);
      
      maxMapSlots = mrInfo.maxMapSlots;
      maxReduceSlots = mrInfo.maxReduceSlots;
      mapTaskMem = mrInfo.mapTaskMem;
      reduceTaskMem = mrInfo.reduceTaskMem;
      
      // carve available memory between map and reduce slots
      if ((maxMapSlots > 0) && (maxReduceSlots > 0)) {
        // the memory requested for map and reduces tasks could be
        // granted. Hence, heap sizes of map and reduce tasks do not 
        // have to be computed in getDefaultMapTaskJvmMem() and
        // getDefaultReduceTaskJvmMem()
        defaultMapTaskMemory = mapTaskMem;
        mapSlotsStr = Integer.toString(maxMapSlots);

        defaultReduceTaskMemory = reduceTaskMem;
        reduceSlotsStr = Integer.toString(maxReduceSlots);

        LOG.info("map and reduce slots have been computed based on requested\n" +
                 "heap sizes for map and reduce slots");
        LOG.info("maptask heapsize: " + mapTaskMem);
        LOG.info("reducetask heapsize: " + reduceTaskMem);
      }
      else {
        // just reset them to zero and the default values and the corresponding
        // string form will be set correctly outside this if-else loop
        maxMapSlots = maxReduceSlots = 0;
        LOG.info("requested memory could not be granted to map and reduce slots\n" +
                 "number of slots will default to 1\n");
      }
    }

    if (maxMapSlots <= 0) {
      maxMapSlots = 1;
      mapSlotsStr = "1";
    }
    if (maxReduceSlots <= 0) {
      maxReduceSlots = 1;
      reduceSlotsStr = "1";
    }

    maxMapPrefetch = (int)
      Math.round(conf.getFloat(TT_PREFETCH_MAPTASKS, 0.0f) * maxMapSlots);

    /* Log info about slots and default mem settings */
    if (LOG.isInfoEnabled()) {
      LOG.info("Map slots " + maxMapSlots + 
               ", Default heapsize for map task " +
               getDefaultMapTaskJvmMem() + " mb");
      LOG.info("Reduce slots " + maxReduceSlots + 
               ", Default heapsize for reduce task " +
               getDefaultReduceTaskJvmMem() + " mb");
      LOG.info("Ephemeral slots " + maxEphemeralSlots +
               ", memory given for each ephemeral slot " +
               ephemeralSlotsMemlimit + " mb");
      LOG.info("Prefetch map slots " + maxMapPrefetch);
    }

    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), null);

    String infoAddr = conf.get("mapred.task.tracker.http.address");
    if (infoAddr.split(":",2).length != 2) { 
      throw new IllegalArgumentException("Invalid address/port: " + infoAddr);
    }

    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String httpBindAddress = infoSocAddr.getHostName();
    int httpPort = infoSocAddr.getPort();
    this.server = new HttpServer("task", httpBindAddress, httpPort,
        httpPort == 0, conf, aclsManager.getAdminsAcl());
    workerThreads = conf.getInt("tasktracker.http.threads", -1);
    server.setThreads(1, workerThreads);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    this.localDirAllocator = new LocalDirAllocator();
    /* MapR: write to tasktracker.cfg in case LinxuTaskTracker is used */
    if (conf.getBoolean("mapred.tasktracker.task-controller.config.overwrite", 
                        true)) {
      writeTaskControllerConfig(conf);
    }
    Class<? extends TaskController> taskControllerClass = 
      conf.getClass("mapred.task.tracker.task-controller", 
                     LinuxTaskController.class, TaskController.class);
   taskController = 
     (TaskController) ReflectionUtils.newInstance(taskControllerClass, conf);
   taskController.setup(localDirAllocator);

    // create user log manager
    setUserLogManager(new UserLogManager(conf, taskController));

    initialize();
    server.setAttribute("task.tracker", this);
    server.setAttribute("local.file.system", localFs);
    server.setAttribute("conf", conf);
    server.setAttribute("log", LOG);
    server.setAttribute("localDirAllocator", localDirAllocator);
    if (!TaskTracker.useMapRFs(conf)) {
      FILE_CACHE_SIZE = conf.getInt("mapred.tasktracker.file.cache.size", 2000);
      this.shuffleServerMetrics = new ShuffleServerMetrics(conf);
      LOG.info("FILE_CACHE_SIZE for mapOutputServlet set to : " + FILE_CACHE_SIZE);
      server.setAttribute("shuffleServerMetrics", shuffleServerMetrics);
      server.addInternalServlet("mapOutput", "/mapOutput", MapOutputServlet.class);
    }
    server.addServlet("taskLog", "/tasklog", TaskLogServlet.class);
    server.start();
    this.httpPort = server.getPort();
    checkJettyPort(httpPort);
    mapRetainSize = conf.getLong(TaskLogsTruncater.MAP_USERLOG_RETAIN_SIZE, 
        TaskLogsTruncater.DEFAULT_RETAIN_SIZE);
    reduceRetainSize = conf.getLong(TaskLogsTruncater.REDUCE_USERLOG_RETAIN_SIZE,
        TaskLogsTruncater.DEFAULT_RETAIN_SIZE);
  }

  private void checkJettyPort(int port) throws IOException { 
    //See HADOOP-4744
    if (port < 0) {
      shuttingDown = true;
      throw new IOException("Jetty problem. Jetty didn't bind to a " +
      		"valid port");
    }
  }
  
  private void startCleanupThreads() throws IOException {
    taskCleanupThread.setDaemon(true);
    taskCleanupThread.start();
    directoryCleanupThread = CleanupQueue.getInstance();
  }

  // only used by tests
  void setCleanupThread(CleanupQueue c) {
    directoryCleanupThread = c;
  }
  
  CleanupQueue getCleanupThread() {
    return directoryCleanupThread;
  }

  /**
   * The connection to the JobTracker, used by the TaskRunner 
   * for locating remote files.
   */
  public InterTrackerProtocol getJobClient() {
    return jobClient;
  }
        
  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }
    
  /** Queries the job tracker for a set of outputs ready to be copied
   * @param fromEventId the first event ID we want to start from, this is
   * modified by the call to this method
   * @param jobClient the job tracker
   * @return a set of locations to copy outputs from
   * @throws IOException
   */  
  private List<TaskCompletionEvent> queryJobTracker(IntWritable fromEventId,
                                                    JobID jobId,
                                                    InterTrackerProtocol jobClient)
    throws IOException {

    final TaskCompletionEventList events =
      jobClient.getTaskCompletionEventList(jobId, fromEventId.get(),
        probe_sample_size);
    final TaskCompletionEvent[] tceArr = events.getArray();

    //we are interested in map task completion events only. So store
    //only those
    List <TaskCompletionEvent> recentMapEvents =
      new ArrayList<TaskCompletionEvent>();
    for (int i = events.first(); i < events.last(); i++) {
      if (tceArr[i].isMap) {
        recentMapEvents.add(tceArr[i]);
      }
    }
    fromEventId.set(fromEventId.get() + events.length());
    return recentMapEvents;
  }

  /**
   * Main service loop.  Will stay in this loop forever.
   */
  State offerService() throws Exception {
    boolean inRecovery = false;

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();
        // Adjust time
        if (now < lastHeartbeat) {
          lastVolumeHealthCheck = lastStatUpdate = lastHeartbeat = now;
        }
        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        while (waitTime > 0) {
          synchronized (finishedCount) {
            int value = finishedCount.get();
            if (value == 0) { // no slot available
              if (waitTime < 300) {
                finishedCount.wait(waitTime);
                waitTime = 0;
              } else {
                finishedCount.wait(300);
                // wait more 
                waitTime -= 300; 
              }
            } else { 
              // atleast one slot is free 
              finishedCount.set(0);
              break;
            }
          }
        }

        // If the TaskTracker is just starting up:
        // 1. Verify the buildVersion
        // 2. Get the system directory & filesystem
        if(justInited) {
          String jobTrackerBV;
          try {
            jobTrackerBV = jobClient.getBuildVersion();
          } catch (Exception e) {
            LOG.error("Error " + e + " while getting jobTracker build version. Exiting..");
            return State.DENIED;
          }
          String taskTrackerBV = VersionInfo.getBuildVersion();
          if(!taskTrackerBV.equals(jobTrackerBV)) {
            String msg = "Mismatched buildVersions." +
              "\nJobTracker's: " + jobTrackerBV + 
              "\nTaskTracker's: "+ taskTrackerBV;
            LOG.warn(msg);
          }

          String dir; 
          try {
            dir = jobClient.getSystemDir();
          } catch (Exception e) {
            LOG.error("Failed to get system Dir from jobTracker. Exiting..");
            return State.DENIED;
          }
          if (dir == null) {
            throw new IOException("Failed to get system directory");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(fConf);
        }
        
        // Send the heartbeat and process the jobtracker's directives
        HeartbeatResponse heartbeatResponse; 
        try {
          // Send the heartbeat and process the jobtracker's directives
          heartbeatResponse = transmitHeartBeat(now, inRecovery);
        } catch (IOException ioe) {
          LOG.error("Failed to send heartbeat to jobTracker. " + ioe + ". Exiting...");
          return State.DENIED;
        }
        // Note the time when the heartbeat returned, use this to decide when to send the
        // next heartbeat   
        lastHeartbeat = System.currentTimeMillis();
               
        // Check if the map-event list needs purging
        Set<JobID> jobs = heartbeatResponse.getRecoveredJobs();
        if (jobs.size() > 0) {
          synchronized (this) {
            // purge the local map events list
            for (JobID job : jobs) {
              RunningJob rjob;
              synchronized (runningJobs) {
                rjob = runningJobs.get(job);          
                if (rjob != null) {
                  synchronized (rjob) {
                    FetchStatus f = rjob.getFetchStatus();
                    if (f != null) {
                      f.reset();
                    }
                  }
                }
              }
            }

            // Mark the reducers in shuffle for rollback
            synchronized (shouldReset) {
              for (Map.Entry<TaskAttemptID, TaskInProgress> entry 
                   : runningTasks.entrySet()) {
                if (entry.getValue().getStatus().getPhase() == Phase.SHUFFLE) {
                  this.shouldReset.add(entry.getKey());
                }
              }
            }
          }
        }
        
        TaskTrackerAction[] actions = heartbeatResponse.getActions();
        if(LOG.isDebugEnabled()) {
          LOG.debug("Got heartbeatResponse from JobTracker with responseId: " + 
                    heartbeatResponse.getResponseId() + " and " + 
                    ((actions != null) ? actions.length : 0) + " actions");
        }
        if (reinitTaskTracker(actions)) {
          return State.STALE;
        }
            
        // resetting heartbeat interval from the response.
        heartbeatInterval = heartbeatResponse.getHeartbeatInterval();
        justStarted = false;
        justInited = false;
        if (resendTaskReportsAction(actions)) {
          LOG.info("Received a resend action from jobtracker");
          resetReduceTaskEvents();
          inRecovery = true;
          continue;
        } else {
          inRecovery = false;
        }
        if (actions != null){ 
          for(TaskTrackerAction action: actions) {
            if (action instanceof LaunchTaskAction) {
              addToTaskQueue((LaunchTaskAction)action);
            } else if (action instanceof CommitTaskAction) {
              CommitTaskAction commitAction = (CommitTaskAction)action;
              if (!commitResponses.contains(commitAction.getTaskID())) {
                LOG.info("Received commit task action for " + 
                          commitAction.getTaskID());
                commitResponses.add(commitAction.getTaskID());
              }
            } else {
              tasksToCleanup.put(action);
            }
          }
        }
        markUnresponsiveTasks();
        killIdleJvms();
        killOverflowingTasks();
        /** If tasks are launched or slots are released and last 
         *  update was more than 3 secs ago then update Stats file.
         */
        if (slotsChanged && (lastHeartbeat - lastStatUpdate) > 3000) {
          writeTaskTrackerStats();
          slotsChanged = false;
          lastStatUpdate = lastHeartbeat;
        }
        
        /* Check the status of mapr shuffle volume. Stop right away
         * in case volume is not present  or has any errors.
         */
        if (TaskTracker.useMapRFs(fConf) && ttVolumeCheckInterval > 0 && 
            (lastHeartbeat - lastVolumeHealthCheck) > ttVolumeCheckInterval) {
          checkTTVolumeHealth();
          lastVolumeHealthCheck = lastHeartbeat;
        }

        synchronized (this)  {
          if (isRunningWithLowMemory) {
            // If there are no tasks running, come out of degraded mode
            if (mapTotal <= 0 && reduceTotal <= 0) {
              this.isRunningWithLowMemory = false;
              resetCurrentMapSlots(maxMapSlots);
              resetCurrentReduceSlots(maxReduceSlots);
            } else {
              if (lastSlotUpdateTime > 0 && 
                  (lastHeartbeat - lastSlotUpdateTime) > 10000) {
                incrCurrentMapSlots();
                incrCurrentReduceSlots();
                lastSlotUpdateTime = lastHeartbeat;
              }
            }
          }
        }

        //we've cleaned up, resume normal operation
        if (!acceptNewTasks && isIdle()) {
          acceptNewTasks=true;
        }
        //The check below may not be required every iteration but we are 
        //erring on the side of caution here. We have seen many cases where
        //the call to jetty's getLocalPort() returns different values at 
        //different times. Being a real paranoid here.
        checkJettyPort(server.getPort());
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (DiskErrorException de) {
        String msg = "Exiting TaskTracker for disk error: " +
          StringUtils.stringifyException(de);
        LOG.error(msg);
        synchronized (this) {
          jobClient.reportTaskTrackerError(taskTrackerName, 
                                           "DiskErrorException", msg);
        }
        return State.DENIED;
      } catch (RemoteException re) {
        String reClass = re.getClassName();
        if (DisallowedTaskTrackerException.class.getName().equals(reClass)) {
          LOG.info("Tasktracker disallowed by JobTracker.");
          return State.DENIED;
        }
      } catch (Exception except) {
        String msg = "Caught exception: " + 
          StringUtils.stringifyException(except);
        LOG.error(msg);
      }
    }

    return State.NORMAL;
  }

  private long previousUpdate = 0;

  void setIndexCache(IndexCache cache) {
    this.indexCache = cache;
  }

  /**
   * Build and transmit the heart beat to the JobTracker
   * @param now current time
   * @return false if the tracker was unknown
   * @throws IOException
   */
  HeartbeatResponse transmitHeartBeat(long now, boolean inRecovery) 
  throws IOException {
    // Send Counters in the status once every COUNTER_UPDATE_INTERVAL
    boolean sendCounters;
    boolean askForNewTask = false;
    long localMinSpaceStart = 0L;
    int ephemeralSlots = ephemeralTaskLauncher.getFreeSlots();

    if (now > (previousUpdate + COUNTER_UPDATE_INTERVAL)) {
      sendCounters = true;
      previousUpdate = now;
    }
    else {
      sendCounters = false;
    }

    // 
    // Check if the last heartbeat got through... 
    // if so then build the heartbeat information for the JobTracker;
    // else resend the previous status information.
    //
    if (status == null) {
      synchronized (this) {
        if (!inRecovery) {
          status = new TaskTrackerStatus(taskTrackerName, LOCALHOSTNAME, 
              httpPort, 
              cloneAndResetRunningTaskStatuses(sendCounters), 
              failures, 
              getCurrentMaxMapSlots() + getCurrentMaxPrefetchSlots(),
              getCurrentMaxReduceSlots(), 
              getCurrentMaxPrefetchSlots());
          status.setEphemeralSlots(ephemeralSlots);
        } else {
          status = new TaskTrackerStatus(taskTrackerName, LOCALHOSTNAME, 
              httpPort, cloneAllTaskStatuses(),
              failures,
              getCurrentMaxMapSlots() + getCurrentMaxPrefetchSlots(), 
              getCurrentMaxReduceSlots(),
              getCurrentMaxPrefetchSlots()); 
          LOG.warn("Sending a recovery heartbeat");
          status.setRecoveryHB();
        }

        // add fid on all tt statuses to deal with JT restarts.
        status.shuffleRootFid = maprTTLayout.getShuffleRootFid();
      }
    } else {
      if (LOG.isInfoEnabled())
          LOG.info("Resending 'status' to '" + "Jobtracker " +
                   "' with reponseId '" + heartbeatResponseId);
    }
      
    //
    // Check if we should ask for a new Task
    //

    if (ephemeralSlots > 0) { 
      askForNewTask =  true;
    } else {
      synchronized (this) {
        if (maxMapPrefetch > 2) {
          /* For large prefetches:
           * Ask for slots only if no. of maps running is less than 
           * maxMapSlots + 0.5*maxMapPrefetch.
           **/
          askForNewTask = ((status.countOccupiedMapSlots() < 
                (maxMapSlots + ((maxMapPrefetch + 1)/ 2)) || 
                status.countOccupiedReduceSlots() < maxReduceSlots) 
              && acceptNewTasks && canLaunchTask());
          localMinSpaceStart = minSpaceStart;
        } else {
          /* For small prefetches do not worry about if current memory usage
           * is above limit. TaskLauncher will take care of exceeding memory 
           **/
          askForNewTask = 
            ((status.countOccupiedMapSlots() < (maxMapSlots + maxMapPrefetch)||
              status.countOccupiedReduceSlots() < maxReduceSlots) && 
             acceptNewTasks && canLaunchTask());
          localMinSpaceStart = minSpaceStart;
        }
      }
    }
    
    long freeDiskSpace = getFreeSpace();
    boolean isEnoughSpace = minSpaceLeft < freeDiskSpace;
    boolean lowDiskSpace = !isEnoughSpace && (freeDiskSpace > 0L);
    boolean askForNewTaskSpace = localMinSpaceStart < freeDiskSpace;
    final String healthReport;

    // freeDiskSpace == 0L mostly seems like an intermittent issue 
    // with Java's File.getAvailable(). We just don't raise an alarm for this case.
    // If the freeDiskSpace was genuinely down to 0L, we would have raised an alarm way
    // earlier (when it went below 1GB)
    if (freeDiskSpace <= 0L) {
      LOG.warn("Invalid Disk space: " + freeDiskSpace + " bytes reported by the system");
      healthReport = "";
    } else if ( lowDiskSpace ) {
      if ( !askForNewTaskSpace ) {
        healthReport = "CRITICAL: Less than " + localMinSpaceStart / ONE_MB + 
        "MB of free space remaining. Can not accept new tasks";
      } else {
        String[] localDirs = fConf.getLocalDirs();
        if ( localDirs !=  null && localDirs.length > 0 ) {
          healthReport = "WARNING: Less than " + minSpaceLeft / ONE_MB + 
          "MB of free space remaining on " + Arrays.asList(localDirs);
        } else {
          healthReport = "WARNING: Less than " + minSpaceLeft / ONE_MB + 
          "MB of free space remaining";
        }
      }
    } else {
      healthReport = "";
    }

    if ( isEnoughSpace && ( isAlarmRaised == null || isAlarmRaised ) ) {
      // clear alarm
      clearSpaceAlarm(healthReport);
    } else if ( lowDiskSpace && 
               ( isAlarmRaised == null || !isAlarmRaised ) ) {
      // raise alarm
      raiseSpaceAlarm(healthReport);
    } else {
      // really nothing here: if alarm is raised and not enough space is fine
      // if alarm is cleared and enough space it is fine as well
    }

    if (askForNewTask) {
      checkLocalDirs(localFs, fConf.getLocalDirs(), false);
      askForNewTask = localMinSpaceStart < freeDiskSpace;
      long totVmem = getTotalVirtualMemoryOnTT();
      long totPmem = getTotalPhysicalMemoryOnTT();
      //status.getResourceStatus().setAvailableSpace(freeDiskSpace);
      status.getResourceStatus().setAvailableSpace(-1);
      status.getResourceStatus().setTotalVirtualMemory(totVmem);
      status.getResourceStatus().setTotalPhysicalMemory(totPmem);
      status.getResourceStatus().setMapSlotMemorySizeOnTT(
          mapSlotMemorySizeOnTT);
      status.getResourceStatus().setReduceSlotMemorySizeOnTT(
          reduceSlotSizeMemoryOnTT);
    }
    //add node health information
    TaskTrackerHealthStatus healthStatus = status.getHealthStatus();
    healthStatus.setHealthReport(healthReport);
    synchronized (this) {
      if (healthChecker != null) {
        healthChecker.setHealthStatus(healthStatus);
      } else {
        healthStatus.setNodeHealthy(true);
        healthStatus.setLastReported(0L);
      }
    }
    //
    // Xmit the heartbeat
    //
    // AH 
    /*
    HeartbeatResponse heartbeatResponse = jobClient.heartbeat(status, 
                                                              justStarted,
                                                              justInited,
                                                              askForNewTask, 
                                                              heartbeatResponseId);
    */
    HeartbeatResponse heartbeatResponse;
    try {
      heartbeatResponse = jobClient.heartbeat(status, 
                                              justStarted, 
                                              justInited,
                                              askForNewTask, 
                                              heartbeatResponseId);
    } catch (Throwable e) {
      throw new IOException("Heartbeat to JobTracker failed!", e);
    }

    //
    // The heartbeat got through successfully!
    //
    heartbeatResponseId = heartbeatResponse.getResponseId();
      
    synchronized (this) {
      for (TaskStatus taskStatus : status.getTaskReports()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            !taskStatus.inTaskCleanupPhase()) {
          try {
            myInstrumentation.completeTask(taskStatus.getTaskID());
          } catch (MetricsException me) {
            LOG.warn("Caught: " + StringUtils.stringifyException(me));
          }
          removeTask(taskStatus.getTaskID(), taskStatus.getIsMap());
        }
      }
      
      // Clear transient status information which should only
      // be sent once to the JobTracker
      for (TaskInProgress tip: runningTasks.values()) {
        tip.getStatus().clearStatus();
      }
    }

    // Force a rebuild of 'status' on the next iteration
    status = null;

    return heartbeatResponse;
  }

  /**
   * Return the total virtual memory available on this TaskTracker.
   * @return total size of virtual memory.
   */
  long getTotalVirtualMemoryOnTT() {
    return totalVirtualMemoryOnTT;
  }

  /**
   * Return the total physical memory available on this TaskTracker.
   * @return total size of physical memory.
   */
  long getTotalPhysicalMemoryOnTT() {
    return totalPhysicalMemoryOnTT;
  }

  long getTotalMemoryAllottedForTasksOnTT() {
    return totalMemoryAllottedForTasks;
  }

  /**
   * @return The amount of physical memory that will be used for running
   *         tasks in bytes. Returns JobConf.DISABLED_MEMORY_LIMIT if it is not
   *         configured.
   */
  long getReservedPhysicalMemoryOnTT() {
    return reservedPhysicalMemoryOnTT;
  }

  long getRetainSize(org.apache.hadoop.mapreduce.TaskAttemptID tid) {
    return tid.isMap() ? mapRetainSize : reduceRetainSize;
  }
  
  /**
   * Check if the jobtracker directed a 'reset' of the tasktracker.
   * 
   * @param actions the directives of the jobtracker for the tasktracker.
   * @return <code>true</code> if tasktracker is to be reset, 
   *         <code>false</code> otherwise.
   */
  private boolean reinitTaskTracker(TaskTrackerAction[] actions) {
    if (actions != null) {
      for (TaskTrackerAction action : actions) {
        if (action.getActionId() == 
            TaskTrackerAction.ActionType.REINIT_TRACKER) {
          LOG.info("Received RenitTrackerAction from JobTracker");
          return true;
        }
      }
    }
    return false;
  }

  /* Read hostname from /opt/mapr/hostname */
  public static String getMapRHostname() {
    BufferedReader breader = null;
    FileReader     freader = null;
    try {
      freader = new FileReader(hostNameFile);
      breader = new BufferedReader(freader);
      return breader.readLine();
    } catch (Exception e) {
      /* On any exception while reading hostname return null */
      if (LOG.isWarnEnabled()) {
        LOG.warn("Error while reading " + hostNameFile, e);
      }
    } finally {
      try {
        if (breader != null) {
          breader.close();
        }
      } catch (Throwable t) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Failed to close breader", t);
        }
      }

      try {
        if (freader != null) {
          freader.close();
        }
      } catch (Throwable t) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Failed to close " + hostNameFile, t);
        }
      }
    }
    return null;
  }

  /* 
  static float eval( String expr) throws ParseException {
    String tokens[] = expr.split(" ");
    String tok;
    float stack[] = new float[20];
    int tos = 0; // top of stack

    for (int i=0; i< tokens.length; ++i) {
      tok = tokens[i];
      if (tok.equals("*")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand '*' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = v1 * v2;
      }

      else if (tok.equals("+")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand '+' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = v1 + v2;
      }

      else if (tok.equals("-")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand '-' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = v1 - v2;
      }

      else if (tok.equals("/")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand '/' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = v1 / v2;
      }

      else if (tok.equals("min")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand 'min' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = Math.min( v1, v2);
      }

      else if (tok.equals("max")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand 'max' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = Math.max( v1, v2);
      }

      else if (tok.equals("**")) {
        float v1, v2;
        if (tos < 2) {
          throw new ParseException("Not enough args, operand '**' at position " + i, i);
        }
        v2 = stack[ --tos];
        v1 = stack[ --tos];
        stack[ tos++ ] = (float) Math.pow( v1, v2);
      }

      else if (tok.equals("=")) {
        float v1;
        if (tos < 1) {
          throw new ParseException("Not enough args, operand '=' at position " + i, i);
        }
        return stack[ --tos];
      }

      else if (tok.matches("[\\d]+[.]?[\\d]*")) { // number
        stack[ tos++] = Float.parseFloat(tok);
      }
    }
    return stack[ --tos];
  }
  */

  int calculateSlots(String fmt) throws ParseException {
    Map<String, BigDecimal> variables = new HashMap<String, BigDecimal>();
    variables.put("CPUS", new BigDecimal(cpuCount));
    // convert memory to gigabytes
    variables.put("MEM", new BigDecimal((float)reservedMemInMB/1024.0));
    variables.put("DISKS", new BigDecimal(diskCount));
    Expression expression = new Expression(fmt);
    return expression.eval(variables).intValue();
  }
  
  int calcMapSlots(String newStr) throws ParseException {
    if (mapSlotsStr == null) {
      mapSlotsStr = newStr;
    } else {
      if (mapSlotsStr.equals(newStr)) {
        return -1;
      } else {
        mapSlotsStr = newStr;
      }
    } 
    return calculateSlots(mapSlotsStr); 
  }
  
  int calcReduceSlots(String newStr) throws ParseException {
    if (reduceSlotsStr == null) {
      reduceSlotsStr = newStr;
    } else {
      if (reduceSlotsStr.equals(newStr)) {
        return -1;
      } else {
        reduceSlotsStr = newStr;
      }
    }
    return calculateSlots(reduceSlotsStr); 
  }

  /*   TODO: Bug 1492
  void confChanged() {
    File file = new File(ttConfigFile);
    if (file.exists()) {
      long lastModified = file.lastModified();
      if (lastModified > lastReload) {
        lastReload = lastModified;
        Configuration conf = Configuration();
        conf.addResource(ttConfigFile);
        String newMapSlotsStr = conf.get("mapred.tasktracker.map.tasks.maximum");
        String newReduceSlotStr = conf.get("mapred.tasktracker.reduce.tasks.maximum");
        calcMapSlots(newMapSlotsStr);
        calcReduceSlots(newReduceSlotsStr);
        conf = null;
      } 
    } 
  }
  */
  
  /* Read resource info from /opt/mapr/logs/cpu_mem_disk */
  private void getResourceInfo(JobConf conf) {
     URL ttConfigUrl = originalConf.getClassLoader().getResource("mapred-site.xml");
     ttConfigFile = ttConfigUrl.getPath();
     LOG.info("TT local config  is " + ttConfigFile);
     Properties properties = new Properties();
     LOG.info("Loading resource properties file : " + resourcesFile);
     try {
       properties.load(new FileInputStream(resourcesFile));
     } catch (IOException e) {
       /* On any exception while reading hostname return null */
       LOG.warn("Error while reading " + resourcesFile + " : " + e 
                + "It will affect dynamic configuration of map/reduce slots");
       return;
     }
     cpuCount = Integer.valueOf(properties.getProperty("cpus", "0"));
     diskCount = Integer.valueOf(properties.getProperty("disks", "0"));
     mfsCpuCount = Integer.valueOf(properties.getProperty("mfscpus", "0"));
     mfsDiskCount = Integer.valueOf(properties.getProperty("mfsdisks", "0"));
     
     long memKSize = Long.valueOf(properties.getProperty("memK", "0"));
     // store in GB
     totalMemInGB = (float)memKSize/(1024 * 1024);
     // MapR : Bug 3711. Set phyiscal memory limits on this node  
     reservedPhysicalMemoryOnTT = 
       conf.getLong(TaskTracker.TT_RESERVED_PHYSCIALMEMORY_MB, 
                     JobConf.DISABLED_MEMORY_LIMIT);
     if (reservedPhysicalMemoryOnTT == JobConf.DISABLED_MEMORY_LIMIT) {
       // use memory set by warden
       String reservedMem = System.getProperty("tt.tasks.mem");
       if (reservedMem != null) {
         reservedMemInMB = Integer.valueOf(reservedMem);
       } else {
         // use all memory
         reservedMemInMB = (memKSize/1024);
       }
     } else {
       // use memory set in mapred-site.xml
       reservedMemInMB = reservedPhysicalMemoryOnTT;
     }
     // set memory aside for ephemeral slots
     if (reservedMemInMB <= getEphemeralSlotsMemReq()) {
       // Cancel ephemeral slots
       maxEphemeralSlots = 0;
     } else {
       reservedMemInMB -= getEphemeralSlotsMemReq();
     }
     reservedPhysicalMemoryOnTT = (reservedMemInMB + maxMapSlots + maxReduceSlots
                                  + getEphemeralSlotsMemReq());
     reservedPhysicalMemoryOnTT *= (1024 * 1024); // normalize to bytes
     LOG.info("Physical memory reserved for mapreduce tasks = " + 
              reservedPhysicalMemoryOnTT + " bytes");
     LOG.info("CPUS: " + cpuCount);
     LOG.info("mfsCPUS: " + mfsCpuCount);
     LOG.info("Total MEM: " + totalMemInGB + "GB");
     LOG.info("Reserved MEM: " + reservedMemInMB + "MB");
     LOG.info("Reserved MEM for Ephemeral slots " + getEphemeralSlotsMemReq());
     LOG.info("DISKS: " + diskCount);
     LOG.info("mfsDISKS: " + mfsDiskCount);
     return;
  }

  private String checkJvmMonitoringOption(String opt) {
    // If not set by user/admin set default based on /tmp
    if (!opt.contains("-XX:+UsePerfData") &&
        !opt.contains("-XX:-UsePerfData")) {
      if (shouldUsePerfData()) {
        return opt + " -XX:+UsePerfData";
      } else {
        return opt + " -XX:-UsePerfData";
      }
    }
    return opt;
  }

  public int getEphemeralSlotsMemReq () { 
   return ephemeralSlotsMemlimit * maxEphemeralSlots;
  } 

  public int getEphemeralSlotsMemlimit () {
    return ephemeralSlotsMemlimit;
  }

  /* Called by TT to see if it has to ask for new task.
   * If map/reduce launcher is waiting don't ask for more tasks.
   */
  private synchronized void launcherWaiting(boolean flag) {
    this.launcherWaiting = flag;
  }

  private synchronized boolean isLauncherWaiting() {
    return this.launcherWaiting;
  }

  /* Called by TT. Min memory required for a task is 200mb.
   */
  private boolean canLaunchTask() {
    if (reservedMemInMB < 200)
      return false;
    
    /* If memory limit is exceeded do not ask for more slots.
     * It may affect prefetch but we want to slow down tasks launched.
     */
    // BUG 3711
    if (maxHeapSizeBasedChecking) {
      synchronized(currentMemoryUsed) {
        if (currentMemoryUsed.get() > reservedMemInMB) {
          return false;
        }
      }
    }
    return true;
  }

  /* Reserve memory  for the task before its launched.
   * Task launcher can wait in this call.
   */
  private void reserveTaskMemory(int memory, boolean isMap) throws IOException {
    int memoryAllocated = Integer.MAX_VALUE;
    while(true) {
      int runningMaps = getRunningMapTasksCount();
      int runningReducers = getRunningReduceTasksCount();
      synchronized(currentMemoryUsed) {
        memoryAllocated  = currentMemoryUsed.get();
        /* Add 1mb per slot to memory reserved.
         * Since per task memory is rounded to nearest integer there is
         * a chance that the last task will wait unnecessarily.
         * If this is the first map task or reduce task allow it to launch even if 
         * max memory limit exceeds.
         **/
        if (((isMap && (runningMaps > 0)) || 
             (!isMap && (runningReducers > 0))) &&
            ((memoryAllocated + memory) > 
             (reservedMemInMB + (maxMapSlots + maxReduceSlots)))) {
          try {
            if (LOG.isInfoEnabled()) 
              LOG.info("Task Launcher waiting in reserving memory " + 
                        memory + "mb. Current memory in usage " + 
                        memoryAllocated + "mb");
            launcherWaiting(true);
            currentMemoryUsed.wait();
            launcherWaiting(false);
          } catch (InterruptedException ie) { 
            // abort 
            throw new IOException("Received InterruptedException " + ie + 
                                  " while reserving task memory: " + memory + 
                                  " Aborting task launch.");
          }
        } else {
          currentMemoryUsed.set(memoryAllocated + memory);
          break;
        }
      }
    }
  }


  /* Release memory reserved for the task before slot is released */
  private void freeTaskMemory(int memory) {
    int memoryAllocated = 0;
    synchronized(currentMemoryUsed) {
      memoryAllocated  = currentMemoryUsed.get();
      if (memoryAllocated - memory > 0) {
        currentMemoryUsed.set(memoryAllocated - memory);
      } else {
        currentMemoryUsed.set(0);
      }
      currentMemoryUsed.notifyAll();
    }
  }

  private int getTaskMemoryRequired(JobConf conf, boolean isMapTask, 
                                    boolean isEphemeral) {
    String opt = null;
    int mem = 0;
    if (isMapTask) {
      opt = conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS,
                     conf.get(JobConf.MAPRED_TASK_JAVA_OPTS));
      if (!opt.contains("-Xmx")) {
        if (isEphemeral) {
          mem = getEphemeralSlotsMemlimit();
        } else {
          mem = getDefaultMapTaskJvmMem();
        }
        opt = opt + " -Xmx" + mem + "m";
      } else {
        mem = conf.extractMaxHeapSize(opt);
      }
      // Even if MAPRED_TASK_JAVA_OPTS is set we set MAPRED_MAP_TASK_JAVA_OPTS
      // TaskRunners will always look for MAPRED_MAP_TASK_JAVA_OPTS first.
      conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS,
               checkJvmMonitoringOption(opt).trim());
    } else {
      opt = conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS,
                     conf.get(JobConf.MAPRED_TASK_JAVA_OPTS, ""));
      if (!opt.contains("-Xmx")) {
        if (isEphemeral) {
          mem = getEphemeralSlotsMemlimit();
        } else {
          mem = getDefaultReduceTaskJvmMem();
        }
        opt = opt + " -Xmx" + mem + "m";
      } else {
        mem = conf.extractMaxHeapSize(opt);
      }
      // Even if MAPRED_TASK_JAVA_OPTS is set we set MAPRED_REDUCE_TASK_JAVA_OPTS
      // TaskRunners will always look for MAPRED_REDUCE_TASK_JAVA_OPTS first.
      conf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, 
               checkJvmMonitoringOption(opt).trim());
    }
    return mem;
  }

  /* configure memory for map task on this task tracker */
  public int getDefaultMapTaskJvmMem() {
    if (this.defaultMapTaskMemory == -1) {
      float mapRatio = (float) maxMapSlots /
                       (float) (maxMapSlots + 1.3*maxReduceSlots);
      this.defaultMapTaskMemory = 
        Math.round(reservedMemInMB * mapRatio / maxMapSlots);
    }
    return this.defaultMapTaskMemory;
  }
  
  /* configure memory for reduce task on this task tracker */
  public int getDefaultReduceTaskJvmMem() {
    if (this.defaultReduceTaskMemory == -1) {
      float reduceRatio = (float) (1.3*maxReduceSlots) /
                          (float) (maxMapSlots + 1.3*maxReduceSlots);
      this.defaultReduceTaskMemory = 
        Math.round(reservedMemInMB * reduceRatio / maxReduceSlots);
    }
    return this.defaultReduceTaskMemory;
  }

  /**
   * Kill any tasks that have not reported progress in the last X seconds.
   */
  private synchronized void markUnresponsiveTasks() throws IOException {
    long now = System.currentTimeMillis();
    List <TaskInProgress> orphanTips = new ArrayList<TaskInProgress>();
    for (TaskInProgress tip: runningTasks.values()) {
      if (tip == null) { // MapR should never happen
        LOG.info("Null tip in runningTasks");
        continue;
      }
      // Check if we have a tip with no known running job.
      // Purge task and remove this tip asap.
      // Its safe to lock runningJobs here since TT is locked 
      RunningJob rjob = null;
      synchronized (runningJobs) {
        rjob = runningJobs.get(tip.getTask().getJobID());
      }
      if (rjob == null) {
        String msg = "Killing task " + tip.getTask().getTaskID() 
          + " since job " +  tip.getTask().getJobID() + " is unknown to TaskTracker.";
        LOG.info(msg);
        tip.reportDiagnosticInfo(msg);
        purgeTask(tip, false, true);
        orphanTips.add(tip);
      }

      // for tasks scheduled on ephemeral check startTime, unless cleanup
      final Task task = tip.getTask();
      if (tip.isEphemeral()
       && !task.isTaskCleanupTask()
       && !task.isJobCleanupTask())
      {
        long started = tip.getStartTime();
        // ephemeral task is not launched yet
        if (started != -1) {
          if (!tip.wasKilled && ((now - started) > ephemeralSlotsTimeout)) {
            if (tip.getRunState() != TaskStatus.State.SUCCEEDED) {
              // task is past its time see if its still running
              LOG.info("Killing epehemeral task " + tip.getTask().getTaskID());
              tip.reportDiagnosticInfo("Task " + tip.getTask().getTaskID() +
                  " is preempted(killed) because it took more than " + 
                  ephemeralSlotsTimeout + "ms on ephemeral slot");
              // kill this attempt right away
              purgeTask(tip, false, true);
            }
          }
        }
      } else if (tip.getRunState() == TaskStatus.State.RUNNING ||
                 tip.getRunState() == TaskStatus.State.COMMIT_PENDING ||
                 tip.isCleaningup()) {
        // Check the per-job timeout interval for tasks;
        // an interval of '0' implies it is never timed-out
        long jobTaskTimeout = tip.getTaskTimeout();
        long pingTimeout = tip.getPingTimeout();
          
        // Check if the task has not reported progress for a 
        // time-period greater than the configured time-out
        long timeSinceLastReport = now - tip.getLastProgressReport();
        long timeSinceLastPing = now - tip.getLastPing();
        if (jobTaskTimeout > 0 && timeSinceLastReport > jobTaskTimeout && !tip.wasKilled) {
          // tell runner to collect diagnostics before killing the task
          TaskRunner runner = tip.getTaskRunner();
          String msg = "";
          if (runner != null) {
            runner.collectDiagnostics(tip.getDiagnosticsDir());
            msg = "Task " + tip.getTask().getTaskID()
                  + " failed to report status for "
                  + (timeSinceLastReport / 1000) + " seconds. Killing!. "
                  + "Diagnostic information will be saved in userlogs";
          } else {
            msg = "Task " + tip.getTask().getTaskID()
                  + " failed to report status for "
                  + (timeSinceLastReport / 1000) + " seconds. Killing!. ";
          }
          LOG.info(tip.getTask().getTaskID() + ": " + msg);
          /** MapR: Do not print TT's thread info */
          // ReflectionUtils.logThreadInfo(LOG, "lost task", 30);
          tip.reportDiagnosticInfo(msg);
          myInstrumentation.timedoutTask(tip.getTask().getTaskID());
          purgeTask(tip, true, true);
        } else if (pingTimeout > 0 && timeSinceLastPing > pingTimeout && !tip.wasKilled) {
          String msg = "Task " + tip.getTask().getTaskID()
                + " failed to ping TT for "
                + (timeSinceLastPing / 1000) + " seconds. Killing!";
          LOG.info(msg);
          tip.reportDiagnosticInfo(msg);
          myInstrumentation.timedoutTask(tip.getTask().getTaskID());
          purgeTask(tip, true, true);
        }
      }
    }
    // cleanup orphans, TT is locked
    for (TaskInProgress tip : orphanTips) {
      removeTask(tip.getTask().getTaskID(), tip.getTask().isMapTask());
    }
  }

  /**
   * The task tracker is done with this job, so we need to clean up.
   * @param action The action with the job
   * @throws IOException
   */
  synchronized void purgeJob(KillJobAction action) throws IOException {
    JobID jobId = action.getJobID();
    LOG.info("Received 'KillJobAction' for job: " + jobId);
    RunningJob rjob = null;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);
    }
      
    if (rjob == null) {
      LOG.warn("Unknown job " + jobId + " being deleted.");
    } else {
      synchronized (rjob) {
        rjob.markKilled();
        // decrement the reference counts for the items this job references
        rjob.distCacheMgr.release();
        // Add this tips of this job to queue of tasks to be purged 
        for (TaskInProgress tip : rjob.tasks) {
          tip.jobHasFinished(false, true);
          Task t = tip.getTask();
          if (indexCache != null && t.isMapTask()) {
            indexCache.removeMap(tip.getTask().getTaskID().toString());
          }
          // Always remove these tasks from TT's state. 
          // There will be a close race between cleanup tasks added in same heartbeat as killJobAction
          removeTask(tip.getTask().getTaskID(), tip.getTask().isMapTask());
        }
        // Delete the job directory for this  
        // task if the job is done/failed
        if (rjob.localized && !rjob.keepJobFiles) {
          removeJobFiles(rjob.ugi.getShortUserName(), rjob.getJobID());
          // remove job dirs on maprfs
          if (TaskTracker.useMapRFs(fConf)) {
            // remove job dirs on maprfs
            FileSystem fs = FileSystem.get(fConf);
            // AH for CDH3
            final PathDeletionContext[] contexts =
              new CleanupQueue.PathDeletionContext[] {
                new PathDeletionContext(
                  maprTTLayout.
                    getMapRJobOutputDir(rjob.getJobID()).
                      makeQualified(fs), fConf),
                new PathDeletionContext(
                  maprTTLayout.getMapRJobSpillDir(rjob.getJobID()).
                    makeQualified(fs), fConf),
                new PathDeletionContext(
                  maprTTLayout.
                    getMapRJobUncompressedOutputDir(rjob.getJobID()).
                      makeQualified(fs), fConf),
                new PathDeletionContext(
                  maprTTLayout.
                    getMapRJobUncompressedSpillDir(rjob.getJobID()).
                      makeQualified(fs), fConf)
              };

            directoryCleanupThread.addToQueue(contexts);
            /* AH apache change 
            directoryCleanupThread.addToQueue(fConf,
                maprTTLayout.getMapRJobOutputDir(rjob.getJobID()).makeQualified(fs));
            directoryCleanupThread.addToQueue(fConf,
                maprTTLayout.getMapRJobSpillDir(rjob.getJobID()).makeQualified(fs));
            directoryCleanupThread.addToQueue(fConf,
                maprTTLayout.getMapRJobUncompressedOutputDir(rjob.getJobID()).makeQualified(fs));
            directoryCleanupThread.addToQueue(fConf,
                maprTTLayout.getMapRJobUncompressedSpillDir(rjob.getJobID()).makeQualified(fs));
            */    
          }
        }
        int retainHoursMax = UserLogCleaner.getUserlogRetainHoursMax(getJobConf());
        int retainHours = UserLogCleaner.getUserlogRetainHours(rjob.getJobConf());

        // use the user specified value for userlog retain hours only if it is within the max limit
        if (retainHours > retainHoursMax) {
          LOG.warn("The value for " + JobContext.USER_LOG_RETAIN_HOURS +  " specified for " + 
                    rjob.getJobID() + " (" + retainHours + ") is higher than the " + 
                    "max allowed value for this cluster (" + retainHoursMax + "). " + 
                    "The logs for this job will be deleted after " + retainHoursMax + " hours.");
          retainHours = retainHoursMax;
        }

        // add job to user log manager
        long now = System.currentTimeMillis();
        JobCompletedEvent jca = new JobCompletedEvent(rjob
            .getJobID(), now, retainHours);
        getUserLogManager().addLogEvent(jca);

        // Remove this job 
        rjob.tasks.clear();
        // Close all FileSystems for this job
        try {
          FileSystem.closeAllForUGI(rjob.getUGI());
        } catch (IOException ie) {
          LOG.warn("Ignoring exception " + StringUtils.stringifyException(ie) + 
              " while closing FileSystem for " + rjob.getUGI());
        }
      }
    }

    synchronized(runningJobs) {
      runningJobs.remove(jobId);
    }
    getJobTokenSecretManager().removeTokenForJob(jobId.toString());  
    distributedCacheManager.removeTaskDistributedCacheManager(jobId);
  }

  /**
   * This job's files are no longer needed on this TT, remove them.
   *
   * @param rjob
   * @throws IOException
   */
  void removeJobFiles(String user, JobID jobId) throws IOException {
    String userDir = getUserDir(user);
    String jobDir = getLocalJobDir(user, jobId.toString());
    PathDeletionContext jobCleanup = 
      new TaskController.DeletionContext(getTaskController(), false, user, 
                                         jobDir.substring(userDir.length()));
    directoryCleanupThread.addToQueue(jobCleanup);
    
    for (String str : localdirs) {
      Path ttPrivateJobDir = FileSystem.getLocal(fConf).makeQualified(
        new Path(str, TaskTracker.getPrivateDirForJob(user, jobId.toString())));
      PathDeletionContext ttPrivateJobCleanup =
        new CleanupQueue.PathDeletionContext(ttPrivateJobDir, fConf);
      directoryCleanupThread.addToQueue(ttPrivateJobCleanup);
    }
  }
  
  /**
   * Remove job files from TaskTracker's private cache.
   * Called when job initialization fails.
   */
  void removeJobPrivateFilesNow(String user, JobID jobId) throws IOException {
    Throwable firstEx = null;
    final String userJobDir =
      TaskTracker.getPrivateDirForJob(user, jobId.toString());

    for (String str : localdirs) {
      final Path ttPrivateJobDir = new Path(str, userJobDir);
      try {
        if (localFs.exists(ttPrivateJobDir)
         && !localFs.delete(ttPrivateJobDir, true))
        {
          if (LOG.isErrorEnabled()) {
            LOG.error("Failed to delete " + ttPrivateJobDir);
          }
        }
      } catch (Throwable t) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Failed to delete " + ttPrivateJobDir, t);
        }
        if (firstEx != null) firstEx = t;
      }
    }

    if (firstEx instanceof IOException) {
      throw (IOException)firstEx;
    } else if (firstEx != null) {
      throw new IOException("Failed to delete job filesi "
        + Arrays.toString(localdirs) + "/" + userJobDir, firstEx);
    }
  }

  /**
   * Remove the tip and update all relevant state.
   * 
   * @param tip {@link TaskInProgress} to be removed.
   * @param wasFailure did the task fail or was it killed?
   */
  private void purgeTask(TaskInProgress tip, boolean wasFailure, boolean killNow) 
  throws IOException {
    if (tip != null) {
      LOG.info("About to purge task: " + tip.getTask().getTaskID());
        
      // Remove the task from running jobs, 
      // removing the job if it's the last task
      removeTaskFromJob(tip.getTask().getJobID(), tip);
      tip.jobHasFinished(wasFailure, killNow);
      if (indexCache != null && tip.getTask().isMapTask()) {
        indexCache.removeMap(tip.getTask().getTaskID().toString());
      }
    }
  }

  /** Check if we're dangerously low on disk space
   * If so, kill jobs to free up space and make sure
   * we don't accept any new tasks
   * Try killing the reduce jobs first, since I believe they
   * use up most space
   * Then pick the one with least progress
   */
  private void killOverflowingTasks() throws IOException {
    long localMinSpaceKill;
    synchronized(this){
      localMinSpaceKill = minSpaceKill;  
    }
    if (!enoughFreeSpace(localMinSpaceKill)) {
      acceptNewTasks=false; 
      //we give up! do not accept new tasks until
      //all the ones running have finished and they're all cleared up
      synchronized (this) {
        TaskInProgress killMe = findTaskToKill(null);

        if (killMe!=null) {
          String msg = "Tasktracker running out of space." +
            " Killing task.";
          LOG.info(killMe.getTask().getTaskID() + ": " + msg);
          killMe.reportDiagnosticInfo(msg);
          purgeTask(killMe, false, true);
        }
      }
    }
  }

  /**
   * Pick a task to kill to free up memory/disk-space 
   * @param tasksToExclude tasks that are to be excluded while trying to find a
   *          task to kill. If null, all runningTasks will be searched.
   * @return the task to kill or null, if one wasn't found
   */
  synchronized TaskInProgress findTaskToKill(List<TaskAttemptID> tasksToExclude) {
    TaskInProgress killMe = null;
    for (Iterator it = runningTasks.values().iterator(); it.hasNext();) {
      TaskInProgress tip = (TaskInProgress) it.next();

      if (tasksToExclude != null
          && tasksToExclude.contains(tip.getTask().getTaskID())) {
        // exclude this task
        continue;
      }

      if ((tip.getRunState() == TaskStatus.State.RUNNING ||
           tip.getRunState() == TaskStatus.State.COMMIT_PENDING) &&
          !tip.wasKilled) {
                
        if (killMe == null) {
          killMe = tip;

        } else if (!tip.getTask().isMapTask()) {
          //reduce task, give priority
          if (killMe.getTask().isMapTask() || 
              (tip.getTask().getProgress().get() < 
               killMe.getTask().getProgress().get())) {

            killMe = tip;
          }
        } else if (killMe.getTask().isMapTask() &&
                   tip.getTask().getProgress().get() < 
                   killMe.getTask().getProgress().get()) {
          //map task, only add if the progress is lower

          killMe = tip;
        }
      }
    }
    return killMe;
  }

  /**
   * Check if any of the local directories has enough
   * free space  (more than minSpace)
   * 
   * If not, do not try to get a new task assigned 
   * @return
   * @throws IOException 
   */
  private boolean enoughFreeSpace(long minSpace) throws IOException {
    if (minSpace == 0L) {
      return true;
    }
    return minSpace < getFreeSpace();
  }
  
  private long getFreeSpace() throws IOException {
    long biggestSeenSoFar = 0L;
    String[] localDirs = fConf.getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      DF df = null;
      if (localDirsDf.containsKey(localDirs[i])) {
        df = localDirsDf.get(localDirs[i]);
      } else {
        df = new DF(new File(localDirs[i]), fConf);
        localDirsDf.put(localDirs[i], df);
      }

      long availOnThisVol = df.getAvailable();
      if (availOnThisVol > biggestSeenSoFar) {
        biggestSeenSoFar = availOnThisVol;
      }
    }
    
    //Should ultimately hold back the space we expect running tasks to use but 
    //that estimate isn't currently being passed down to the TaskTrackers    
    return biggestSeenSoFar;
  }
    
  private TaskLauncher mapLauncher;
  private TaskLauncher reduceLauncher;
  private TaskLauncher ephemeralTaskLauncher;
  public JvmManager getJvmManagerInstance() {
    return jvmManager;
  }

  // called from unit test  
  void setJvmManagerInstance(JvmManager jvmManager) {
    this.jvmManager = jvmManager;
  }

  private void addToTaskQueue(LaunchTaskAction action) {
    if (action.isEphemeral()) {
      ephemeralTaskLauncher.addToTaskQueue(action);
    } else {
      if (action.getTask().isMapTask()) {
        mapLauncher.addToTaskQueue(action);
      } else {
        reduceLauncher.addToTaskQueue(action);
      }
    }
  }
  
  class TaskLauncher extends Thread {
    private IntWritable numFreeSlots;
    private final int maxSlots;
    private List<TaskInProgress> tasksToLaunch;
    private boolean shouldExit = false;
    private TaskType taskType;

    public TaskLauncher(TaskType taskType, int numSlots) {
      this.maxSlots = numSlots;
      this.numFreeSlots = new IntWritable(numSlots);
      this.tasksToLaunch = new LinkedList<TaskInProgress>();
      this.taskType = taskType;
      setDaemon(true);
      setName("TaskLauncher for " + taskType + " tasks");
    }

    public void addToTaskQueue(LaunchTaskAction action) {
      synchronized (tasksToLaunch) {
        TaskInProgress tip = registerTask(action, this);
        // set startTime here. It'll kill task if tasklauncher is stuck for any reason. 
        if (this.taskType == TaskType.EPHEMERAL) {
          tip.setStartTime(System.currentTimeMillis());
        }
        tasksToLaunch.add(tip);
        tasksToLaunch.notifyAll();
      }
    }

    public TaskType getTaskType() {
      return taskType;
    }

    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }
    
    public void addFreeSlots(int numSlots) {
      synchronized (numFreeSlots) {
        numFreeSlots.set(numFreeSlots.get() + numSlots);
        assert (numFreeSlots.get() <= maxSlots);
        LOG.info("addFreeSlot : current free slots : " + numFreeSlots.get());
        numFreeSlots.notifyAll();
      }
    }
    
    void notifySlots() {
      synchronized (numFreeSlots) {
        numFreeSlots.notifyAll();
      }
    }

    int getNumWaitingTasksToLaunch() {
      synchronized (tasksToLaunch) {
        return tasksToLaunch.size();
      }
    }

    int getFreeSlots() {
      synchronized (numFreeSlots) {
        return numFreeSlots.get();
      }
    }

    public void shutdown() {
      this.shouldExit = true;
    }

    public void run() {
      while (!shouldExit) {
        try {
          TaskInProgress tip;
          Task task;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            tip = tasksToLaunch.remove(0);
            task = tip.getTask();
            LOG.info("Trying to launch : " + tip.getTask().getTaskID() + 
                     " which needs " + task.getNumSlotsRequired() + " slots");
          }
          //wait for free slots to run
          synchronized (numFreeSlots) {
            boolean canLaunch = true;
            while (numFreeSlots.get() < task.getNumSlotsRequired()) {
              //Make sure that there is no kill task action for this task!
              //We are not locking tip here, because it would reverse the
              //locking order!
              //Also, Lock for the tip is not required here! because :
              // 1. runState of TaskStatus is volatile
              // 2. Any notification is not missed because notification is
              // synchronized on numFreeSlots. So, while we are doing the check,
              // if the tip is half way through the kill(), we don't miss
              // notification for the following wait().
              if (!tip.canBeLaunched()) {
                //got killed externally while still in the launcher queue
                LOG.info("Not blocking slots for " + task.getTaskID()
                    + " as it got killed externally. Task's state is "
                    + tip.getRunState());
                canLaunch = false;
                break;
              }
              LOG.info("TaskLauncher : Waiting for " + task.getNumSlotsRequired() + 
                       " to launch " + task.getTaskID() + ", currently we have " + 
                       numFreeSlots.get() + " free slots");
              numFreeSlots.wait();
            }
            if (!canLaunch) {
              continue;
            }
            LOG.info("In TaskLauncher, current free slots : " + numFreeSlots.get()+
                     " and trying to launch "+tip.getTask().getTaskID() + 
                     " which needs " + task.getNumSlotsRequired() + " slots");
            numFreeSlots.set(numFreeSlots.get() - task.getNumSlotsRequired());
            assert (numFreeSlots.get() >= 0);
          }
          if (!tip.isCleaningup() && tip.isAborted()) {
            String msg = "Killing " + task.getTaskID() + " as job " + 
              task.getTaskID().getJobID() + 
              " is temporarily ignored on this node due to low memory. Task's state is " 
              + tip.getRunState(); 
            // Cancel all unassigned tasks for this job 
            tip.reportDiagnosticInfo(msg);
            try {
              purgeTask(tip, false, true); // Marking it as killed.
            } catch (IOException ioe) {
              LOG.warn("Couldn't purge the task of " + task.getTaskID() + ". Error : " + ioe);
            }
            addFreeSlots(task.getNumSlotsRequired());
            continue;
          }

          synchronized (tip) {
            //to make sure that there is no kill task action for this
            if (!tip.canBeLaunched()) {
              //got killed externally while still in the launcher queue
              LOG.info("Not launching task " + task.getTaskID() + " as it got"
                + " killed externally. Task's state is " + tip.getRunState());
              addFreeSlots(task.getNumSlotsRequired());
              continue;
            }
            tip.slotTaken = true;
          }
          //got a free slot. launch the task
          startNewTask(tip);
          if (waitAfterTaskLaunch) {
            sleep(heartbeatInterval);
          }
        } catch (InterruptedException e) { 
          if (shouldExit) 
            return;
          LOG.warn ("Unexpected InterruptedException");
        } catch (Throwable th) {
          LOG.error("TaskLauncher error " + 
              StringUtils.stringifyException(th));
        }
      }
    }
  }
  private TaskInProgress registerTask(LaunchTaskAction action, 
      TaskLauncher launcher) {
    Task t = action.getTask();
    LOG.info("LaunchTaskAction (registerTask): " + t.getTaskID() +
             " task's state:" + t.getState());
    TaskInProgress tip = new TaskInProgress(t, this.fConf, launcher);
    synchronized (this) {
      JobID jobId = t.getJobID();
      synchronized (runningJobs) {
        if (!runningJobs.containsKey(jobId)) {
          RunningJob rJob = new RunningJob(jobId);
          rJob.tasks = new HashSet<TaskInProgress>();
          runningJobs.put(jobId, rJob);
        }
      }
      tasks.put(t.getTaskID(), tip);
      runningTasks.put(t.getTaskID(), tip);
      boolean isMap = t.isMapTask();
      slotsChanged = true;
      if (isMap) {
        increaseMapTask(1);
      } else {
        increaseReduceTask(1);
      }
    }
    return tip;
  }

  // Reset all reduce tasks' shuffle events
  synchronized void resetReduceTaskEvents() {
    for (TaskInProgress tip: runningTasks.values()) {
      Task task = tip.getTask();
      if (task.isMapTask() == false) {
        shouldReset.add(task.getTaskID());
      }
    }
    this.mapEventsFetcher.interrupt();
    for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
      RunningJob rjob = item.getValue();
      JobID jobId = item.getKey();
      FetchStatus f;
      synchronized (rjob) {
        f = rjob.getFetchStatus();
        if (f != null) {
          f.reset();
        }
      }
    }
    this.mapEventsFetcher = new MapEventsFetcherThread();
    mapEventsFetcher.setDaemon(true);
    mapEventsFetcher.setName("Map-events fetcher for all reduce tasks " + "on " + taskTrackerName);
    mapEventsFetcher.start();
  }

  /**
   * Start a new task.
   * All exceptions are handled locally, so that we don't mess up the
   * task tracker.
   * @throws InterruptedException 
   */
  void startNewTask(
    final TaskInProgress tip
  ) throws InterruptedException 
  {
    final Thread launchThread = new Thread() {
      @Override
      public void run() {
        try {
          RunningJob rjob = localizeJob(tip);
          tip.getTask().setJobFile(rjob.localizedJobConf.toString());
          // Localization is done. Neither rjob.jobConf nor rjob.ugi can be null
          launchTaskForJob(tip, new JobConf(rjob.jobConf), rjob); 
        } catch (Throwable e) {
          String msg = ("Error initializing " + tip.getTask().getTaskID() + " " + 
                        StringUtils.stringifyException(e));
          LOG.warn(msg);
          tip.reportDiagnosticInfo(msg);
          try {
            tip.kill(true, true);
            tip.cleanup(true);
          } catch (IOException ie2) {
            LOG.info("Error cleaning up " + tip.getTask().getTaskID(), ie2);
          } catch (InterruptedException ie2) {
            LOG.info("Error cleaning up " + tip.getTask().getTaskID(), ie2);
          }
          if (e instanceof Error) {
            LOG.error("TaskLauncher error " + 
                StringUtils.stringifyException(e));
          }
        }
      }
    };
    launchThread.start();
  }

  void addToMemoryManager(TaskAttemptID attemptId, boolean isMap,
                          JobConf conf) {
    if (!isTaskMemoryManagerEnabled()) {
      return; // Skip this if TaskMemoryManager is not enabled.
    }

    // Obtain physical memory limits from the job configuration
    long physicalMemoryLimit = isMap ? 
      conf.getPhysicalMemoryForMapTask():
      conf.getPhysicalMemoryForReduceTask();
    if (physicalMemoryLimit > 0) {
      physicalMemoryLimit *= (1024 * 1024);
    }
    // Obtain virtual memory limits from the job configuration
    long virtualMemoryLimit = isMap ? 
      conf.getMemoryForMapTask() :
      conf.getMemoryForReduceTask();
    if (virtualMemoryLimit > 0) {
      virtualMemoryLimit *= (1024 * 1024);
    }
    
    taskMemoryManager.addTask(attemptId, virtualMemoryLimit,
        physicalMemoryLimit);
  }

  void removeFromMemoryManager(TaskAttemptID attemptId) {
    // Remove the entry from taskMemoryManagerThread's data structures.
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager.removeTask(attemptId);
    }
  }

  /** 
   * Notify the tasktracker to send an out-of-band heartbeat.
   */
  private void notifyTTAboutTaskCompletion(boolean isMap, boolean wasKilled) {
    if (!wasKilled) {
      synchronized (this) {
        if (isRunningWithLowMemory) {
          if (isMap) {
            incrCurrentMapSlots();
          } else {
            incrCurrentReduceSlots();
          }
        }
      }
    }
    slotsChanged = true;
    if (oobHeartbeatOnTaskCompletion) {
      synchronized (finishedCount) {
        int value = finishedCount.get();
        finishedCount.set(value + 1);
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastHeartbeat >= 300) {
          finishedCount.notify();
        }
      }
    }
  }

  /**
   * The server retry loop.  
   * This while-loop attempts to connect to the JobTracker.  It only 
   * loops when the old TaskTracker has gone bad (its state is
   * stale somehow) and we need to reinitialize everything.
   */
  public void run() {
    try {
      getUserLogManager().start();
      startCleanupThreads();
      boolean denied = false;
      while (running && !shuttingDown && !denied) {
        boolean staleState = false;
        try {
          // This while-loop attempts reconnects if we get network errors
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception ex) {
              if (!shuttingDown) {
                // AH TODO mention jt address
                LOG.info("Lost connection to JobTracker [" +
                         "  Retrying...", ex);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          close();
        }
        if (shuttingDown) { return; }
        if (!denied) {
          LOG.warn("Reinitializing local state");
          initialize();
        }
      }
      if (denied) {
        shutdown();
      }
    } catch (IOException iex) {
      LOG.error("Got fatal exception while reinitializing TaskTracker: " +
                StringUtils.stringifyException(iex));
      return;
    }
    catch (InterruptedException i) {
      LOG.error("Got interrupted while reinitializing TaskTracker: " +
          i.getMessage());
      return;
    }
  }
    
  ///////////////////////////////////////////////////////
  // TaskInProgress maintains all the info for a Task that
  // lives at this TaskTracker.  It maintains the Task object,
  // its TaskStatus, and the TaskRunner.
  ///////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    long lastProgressReport;
    long lastPing;
    StringBuffer diagnosticInfo = new StringBuffer();
    private TaskRunner runner;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    boolean aborted = false;
    private JobConf ttConf;
    private JobConf localJobConf;
    private boolean keepFailedTaskFiles;
    private boolean alwaysKeepTaskFiles;
    private TaskStatus taskStatus; 
    private long taskTimeout = (10 * 60 * 1000);
    private long pingTimeout = (60 * 1000);
    private String debugCommand;
    private volatile boolean slotTaken = false;
    private TaskLauncher launcher;
    long startTime = -1;
    
    private int memoryReserved = 0;
    private boolean jvmValidationFailureLogged = false;

    public boolean isJvmValidationFailureLogged() {
      return jvmValidationFailureLogged;
    }

    public void setJvmValidationFailureLogged() {
      jvmValidationFailureLogged = true;
    }

    // The ugi of the user who is running the job. This contains all the tokens
    // too which will be populated during job-localization
    private UserGroupInformation ugi;

    UserGroupInformation getUGI() {
      return ugi;
    }

    void setUGI(UserGroupInformation userUGI) {
      ugi = userUGI;
    }

    public boolean isEphemeral() {
      return (launcher.getTaskType() == TaskType.EPHEMERAL);
    }

    void setStartTime(long time) {
      this.startTime = time;
    }

    long getStartTime() {
      return this.startTime;
    }

    public void abort() {
      this.aborted = true;
    }

    public boolean isAborted() {
      return this.aborted;
    }

    /**
     */
    public TaskInProgress(Task task, JobConf conf) {
      this(task, conf, null);
    }
    
    public TaskInProgress(Task task, JobConf conf, TaskLauncher launcher) {
      this.task = task;
      this.launcher = launcher;
      this.lastProgressReport = System.currentTimeMillis();
      this.lastPing = lastProgressReport;
      this.ttConf = conf;
      localJobConf = null;
      taskStatus = TaskStatus.createTaskStatus(task.isMapTask(), task.getTaskID(), 
                                               0.0f, 
                                               task.getNumSlotsRequired(),
                                               task.getState(),
                                               diagnosticInfo.toString(), 
                                               "initializing",  
                                               getName(), 
                                               task.isTaskCleanupTask() ? 
                                                 TaskStatus.Phase.CLEANUP :  
                                               task.isMapTask()? TaskStatus.Phase.MAP:
                                               TaskStatus.Phase.SHUFFLE,
                                               task.getCounters()); 
      this.startTime = -1;
    }
        
    void localizeTask(Task task) throws IOException{
      // Do the task-type specific localization
      //TODO: are these calls really required
      task.localizeConfiguration(localJobConf);
      task.setConf(localJobConf);
    }
        
    /**
     */
    public Task getTask() {
      return task;
    }
    
    synchronized TaskRunner getTaskRunner() {
      return runner;
    }

    public String getDiagnosticsDir() {
      return TaskLog.getAttemptDir(getTask().getTaskID(),
          getTask().isTaskCleanupTask()) + "";
    }

    synchronized void setTaskRunner(TaskRunner rnr) {
      this.runner = rnr;
    }

    void setMemoryReserved(int mem) {
      this.memoryReserved = mem;
    }
    
    public int getMemoryReserved() {
      return this.memoryReserved;
    }

    public synchronized void setJobConf(JobConf lconf){
      this.localJobConf = lconf;
      keepFailedTaskFiles = localJobConf.getKeepFailedTaskFiles();
      taskTimeout = localJobConf.getLong("mapred.task.timeout", 
                                         10 * 60 * 1000);
      pingTimeout = localJobConf.getLong("mapred.task.ping.timeout", 
                                         60 * 1000);
      if (task.isMapTask()) {
        debugCommand = localJobConf.getMapDebugScript();
      } else {
        debugCommand = localJobConf.getReduceDebugScript();
      }
      String keepPattern = localJobConf.getKeepTaskFilesPattern();
      if (keepPattern != null) {
        alwaysKeepTaskFiles = 
          Pattern.matches(keepPattern, task.getTaskID().toString());
      } else {
        alwaysKeepTaskFiles = false;
      }
    }
        
    public synchronized JobConf getJobConf() {
      return localJobConf;
    }
        
    /**
     */
    public synchronized TaskStatus getStatus() {
      taskStatus.setDiagnosticInfo(diagnosticInfo.toString());
      if (diagnosticInfo.length() > 0) {
        diagnosticInfo = new StringBuffer();
      }
      
      return taskStatus;
    }

    /**
     * Kick off the task execution
     */
    public synchronized void launchTask(RunningJob rjob) throws IOException {
      if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED ||
          this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        localizeTask(task);
        if (this.taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
          this.taskStatus.setRunState(TaskStatus.State.RUNNING);
        }
        setTaskRunner(task.createRunner(TaskTracker.this, this, rjob));
        this.runner.start();
        long now = System.currentTimeMillis();
        this.taskStatus.setStartTime(now);
        this.lastProgressReport = now;
        this.lastPing = now;
      } else {
        LOG.info("Not launching task: " + task.getTaskID() + 
            " since it's state is " + this.taskStatus.getRunState());
      }
    }

    boolean isCleaningup() {
      return this.taskStatus.inTaskCleanupPhase();
    }
    
    // checks if state has been changed for the task to be launched
    boolean canBeLaunched() {
      return (getRunState() == TaskStatus.State.UNASSIGNED ||
          getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
          getRunState() == TaskStatus.State.KILLED_UNCLEAN);
    }

    /**
     * The task is reporting its progress
     */
    public synchronized void reportProgress(TaskStatus taskStatus) 
    {
      LOG.info(task.getTaskID() + " " + taskStatus.getProgress() + 
          "% " + taskStatus.getStateString());
      // task will report its state as
      // COMMIT_PENDING when it is waiting for commit response and 
      // when it is committing.
      // cleanup attempt will report its state as FAILED_UNCLEAN/KILLED_UNCLEAN
      if (this.done || 
          (this.taskStatus.getRunState() != TaskStatus.State.RUNNING &&
          this.taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
          !isCleaningup()) ||
          ((this.taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
           this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
           this.taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN) &&
           (taskStatus.getRunState() == TaskStatus.State.RUNNING ||
            taskStatus.getRunState() == TaskStatus.State.UNASSIGNED))) {
        //make sure we ignore progress messages after a task has 
        //invoked TaskUmbilicalProtocol.done() or if the task has been
        //KILLED/FAILED/FAILED_UNCLEAN/KILLED_UNCLEAN
        //Also ignore progress update if the state change is from 
        //COMMIT_PENDING/FAILED_UNCLEAN/KILLED_UNCLEA to RUNNING or UNASSIGNED
        LOG.info(task.getTaskID() + " Ignoring status-update since " +
                 ((this.done) ? "task is 'done'" : 
                                ("runState: " + this.taskStatus.getRunState()))
                 ); 
        return;
      }
      
      this.taskStatus.statusUpdate(taskStatus);
      this.lastProgressReport = System.currentTimeMillis();
      this.lastPing = lastProgressReport;
    }

    public void updateLastProgressReport() {
      this.lastProgressReport = System.currentTimeMillis();
      this.lastPing = lastProgressReport;
    }

    public void updateLastPing() {
      this.lastPing = System.currentTimeMillis();
    }

    /**
     */
    public long getLastProgressReport() {
      return lastProgressReport;
    }

    public long getLastPing() {
      return lastPing;
    }

    /**
     */
    public TaskStatus.State getRunState() {
      return taskStatus.getRunState();
    }

    /**
     * The task's configured timeout.
     * 
     * @return the task's configured timeout.
     */
    public long getTaskTimeout() {
      return taskTimeout;
    }
        
    public long getPingTimeout() {
      return pingTimeout;
    }

    /**
     * The task has reported some diagnostic info about its status
     */
    public synchronized void reportDiagnosticInfo(String info) {
      this.diagnosticInfo.append(info);
    }
    
    public synchronized void reportNextRecordRange(SortedRanges.Range range) {
      this.taskStatus.setNextRecordRange(range);
    }

    /**
     * The task is reporting that it's done running
     */
    public synchronized void reportDone() {
      if (isCleaningup()) {
        if (this.taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.FAILED);
        } else if (this.taskStatus.getRunState() == 
                   TaskStatus.State.KILLED_UNCLEAN) {
          this.taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      } else {
        this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
      }
      this.taskStatus.setProgress(1.0f);
      this.taskStatus.setFinishTime(System.currentTimeMillis());
      this.done = true;
      jvmManager.taskFinished(runner);
      runner.signalDone();
      LOG.info("Task " + task.getTaskID() + " is done.");
      LOG.info("reported output size for " + task.getTaskID() +  
               "  was " + taskStatus.getOutputSize());
    }
    
    public boolean wasKilled() {
      return wasKilled;
    }

    /**
     * A task is reporting in as 'done'.
     * 
     * We need to notify the tasktracker to send an out-of-band heartbeat.
     * If isn't <code>commitPending</code>, we need to finalize the task
     * and release the slot it's occupied.
     * 
     * @param commitPending is the task-commit pending?
     */
    void reportTaskFinished(boolean commitPending) {
      if (!commitPending) {
        taskFinished();
        releaseSlot();
      }
      notifyTTAboutTaskCompletion(task.isMapTask(), wasKilled);
    }

    /* State changes:
     * RUNNING/COMMIT_PENDING -> FAILED_UNCLEAN/FAILED/KILLED_UNCLEAN/KILLED
     * FAILED_UNCLEAN -> FAILED
     * KILLED_UNCLEAN -> KILLED 
     */
    private void setTaskFailState(boolean wasFailure) {
      // go FAILED_UNCLEAN -> FAILED and KILLED_UNCLEAN -> KILLED always
      if (taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else if (taskStatus.getRunState() == 
                 TaskStatus.State.KILLED_UNCLEAN) {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      } else if (task.isMapOrReduce() && 
                 taskStatus.getPhase() != TaskStatus.Phase.CLEANUP) {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED_UNCLEAN);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED_UNCLEAN);
        }
      } else {
        if (wasFailure) {
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
    }
    
    /**
     * The task has actually finished running.
     */
    public void taskFinished() {
      long start = System.currentTimeMillis();

      //
      // Wait until task reports as done.  If it hasn't reported in,
      // wait for a second and try again.
      //
      while (!done && (System.currentTimeMillis() - start < WAIT_FOR_DONE)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }

      //
      // Change state to success or failure, depending on whether
      // task was 'done' before terminating
      //
      boolean needCleanup = false;
      synchronized (this) {
        // Remove the task from MemoryManager, if the task SUCCEEDED or FAILED.
        // KILLED tasks are removed in method kill(), because Kill 
        // would result in launching a cleanup attempt before 
        // TaskRunner returns; if remove happens here, it would remove
        // wrong task from memory manager.
        if (done || !wasKilled) {
          removeFromMemoryManager(task.getTaskID());
          if (maxHeapSizeBasedChecking && !isEphemeral()) {
            freeTaskMemory(getMemoryReserved());
          }
        }
        if (!done) {
          if (!wasKilled) {
            failures += 1;
            setTaskFailState(true);
            // call the script here for the failed tasks.
            if (debugCommand != null) {
              String taskStdout ="";
              String taskStderr ="";
              String taskSyslog ="";
              String jobConf = task.getJobFile();
              try {
                Map<LogName, LogFileDetail> allFilesDetails = TaskLog
                    .getAllLogsFileDetails(task.getTaskID(), task
                        .isTaskCleanupTask(), false);
                // get task's stdout file
                taskStdout =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDOUT).location,
                        LogName.STDOUT);
                // get task's stderr file
                taskStderr =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.STDERR).location,
                        LogName.STDERR);
                // get task's syslog file
                taskSyslog =
                    TaskLog.getRealTaskLogFilePath(
                        allFilesDetails.get(LogName.SYSLOG).location,
                        LogName.SYSLOG);
              } catch(IOException e){
                LOG.warn("Exception finding task's stdout/err/syslog files");
              }
              File workDir = null;
              try {
                workDir =
                    new File(lDirAlloc.getLocalPathToRead(
                        TaskTracker.getLocalTaskDir(task.getUser(), task
                            .getJobID().toString(), task.getTaskID()
                            .toString(), task.isTaskCleanupTask())
                            + Path.SEPARATOR + MRConstants.WORKDIR,
                        localJobConf).toString());
              } catch (IOException e) {
                LOG.warn("Working Directory of the task " + task.getTaskID() +
                                " doesnt exist. Caught exception " +
                          StringUtils.stringifyException(e));
              }
              // Build the command  
              File stdout = TaskLog.getTaskLogFile(task.getTaskID(), task
                  .isTaskCleanupTask(), TaskLog.LogName.DEBUGOUT);
              // add pipes program as argument if it exists.
              String program ="";
              String executable = Submitter.getExecutable(localJobConf);
              if ( executable != null) {
            	try {
            	  program = new URI(executable).getFragment();
            	} catch (URISyntaxException ur) {
            	  LOG.warn("Problem in the URI fragment for pipes executable");
            	}	  
              }
              String [] debug = debugCommand.split(" ");
              Vector<String> vargs = new Vector<String>();
              for (String component : debug) {
                vargs.add(component);
              }
              vargs.add(taskStdout);
              vargs.add(taskStderr);
              vargs.add(taskSyslog);
              vargs.add(jobConf);
              vargs.add(program);
              try {
                List<String>  wrappedCommand = TaskLog.captureDebugOut
                                                          (vargs, stdout);
                // run the script.
                try {
                  runScript(wrappedCommand, workDir);
                } catch (IOException ioe) {
                  LOG.warn("runScript failed with: " + StringUtils.
                                                      stringifyException(ioe));
                }
              } catch(IOException e) {
                LOG.warn("Error in preparing wrapped debug command");
              }

              // add all lines of debug out to diagnostics
              try {
                int num = localJobConf.getInt("mapred.debug.out.lines", -1);
                addDiagnostics(FileUtil.makeShellPath(stdout),num,"DEBUG OUT");
              } catch(IOException ioe) {
                LOG.warn("Exception in add diagnostics!");
              }

              // Debug-command is run. Do the post-debug-script-exit debug-logs
              // processing. Truncate the logs.
              JvmFinishedEvent jvmFinished = new JvmFinishedEvent(new JVMInfo(
                  TaskLog.getAttemptDir(task.getTaskID(), task
                      .isTaskCleanupTask()), Arrays.asList(task)));
              getUserLogManager().addLogEvent(jvmFinished);
            }
          }
          taskStatus.setProgress(0.0f);
        }
        this.taskStatus.setFinishTime(System.currentTimeMillis());
        needCleanup = (taskStatus.getRunState() == TaskStatus.State.FAILED || 
                taskStatus.getRunState() == TaskStatus.State.FAILED_UNCLEAN ||
                taskStatus.getRunState() == TaskStatus.State.KILLED_UNCLEAN || 
                taskStatus.getRunState() == TaskStatus.State.KILLED);
      }

      //
      // If the task has failed, or if the task was killAndCleanup()'ed,
      // we should clean up right away.  We only wait to cleanup
      // if the task succeeded, and its results might be useful
      // later on to downstream job processing.
      //
      if (needCleanup) {
        removeTaskFromJob(task.getJobID(), this);
      }
      try {
        cleanup(needCleanup);
      } catch (IOException ie) {
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("Task " + task.getTaskID() + " exiting, status: " + 
                 taskStatus.getRunState().toString());
      }
    }
    

    /**
     * Runs the script given in args
     * @param args script name followed by its argumnets
     * @param dir current working directory.
     * @throws IOException
     */
    public void runScript(List<String> args, File dir) throws IOException {
      ShellCommandExecutor shexec = 
              new ShellCommandExecutor(args.toArray(new String[0]), dir);
      shexec.execute();
      int exitCode = shexec.getExitCode();
      if (exitCode != 0) {
        throw new IOException("Task debug script exit with nonzero status of " 
                              + exitCode + ".");
      }
    }

    /**
     * Add last 'num' lines of the given file to the diagnostics.
     * if num =-1, all the lines of file are added to the diagnostics.
     * @param file The file from which to collect diagnostics.
     * @param num The number of lines to be sent to diagnostics.
     * @param tag The tag is printed before the diagnostics are printed. 
     */
    public void addDiagnostics(String file, int num, String tag) {
      RandomAccessFile rafile = null;
      try {
        rafile = new RandomAccessFile(file,"r");
        int no_lines =0;
        String line = null;
        StringBuffer tail = new StringBuffer();
        tail.append("\n-------------------- "+tag+"---------------------\n");
        String[] lines = null;
        if (num >0) {
          lines = new String[num];
        }
        while ((line = rafile.readLine()) != null) {
          no_lines++;
          if (num >0) {
            if (no_lines <= num) {
              lines[no_lines-1] = line;
            }
            else { // shift them up
              for (int i=0; i<num-1; ++i) {
                lines[i] = lines[i+1];
              }
              lines[num-1] = line;
            }
          }
          else if (num == -1) {
            tail.append(line); 
            tail.append("\n");
          }
        }
        int n = no_lines > num ?num:no_lines;
        if (num >0) {
          for (int i=0;i<n;i++) {
            tail.append(lines[i]);
            tail.append("\n");
          }
        }
        if(n!=0)
          reportDiagnosticInfo(tail.toString());
      } catch (FileNotFoundException fnfe){
        LOG.warn("File "+file+ " not found");
      } catch (IOException ioe){
        LOG.warn("Error reading file "+file);
      } finally {
         try {
           if (rafile != null) {
             rafile.close();
           }
         } catch (IOException ioe) {
           LOG.warn("Error closing file "+file);
         }
      }
    }
    
    /**
     * We no longer need anything from this task, as the job has
     * finished.  If the task is still running, kill it and clean up.
     * 
     * @param wasFailure did the task fail, as opposed to was it killed by
     *                   the framework
     */
    public void jobHasFinished(boolean wasFailure, boolean killNow) throws IOException {
      // Kill the task if it is still running
      synchronized(this){
        if (getRunState() == TaskStatus.State.RUNNING ||
            getRunState() == TaskStatus.State.UNASSIGNED ||
            getRunState() == TaskStatus.State.COMMIT_PENDING ||
            isCleaningup()) {
          try {
            kill(wasFailure, killNow);
          } catch (InterruptedException e) {
            throw new IOException("Interrupted while killing " +
                getTask().getTaskID(), e);
          }
        }
      }
      
      // Cleanup on the finished task
      cleanup(true);
    }

    /**
     * Something went wrong and the task must be killed.
     * @param wasFailure was it a failure (versus a kill request)?
     * @throws InterruptedException 
     */
    public synchronized void kill(boolean wasFailure, boolean killNow) 
      throws IOException, InterruptedException {
      if (taskStatus.getRunState() == TaskStatus.State.RUNNING ||
          taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING ||
          isCleaningup()) {
        wasKilled = true;
        if (wasFailure) {
          failures += 1;
        }
        // runner could be null if task-cleanup attempt is not localized yet
        if (runner != null) {
          runner.kill(killNow);
        }
        setTaskFailState(wasFailure);
        taskStatus.setFinishTime(System.currentTimeMillis());
      } else if (taskStatus.getRunState() == TaskStatus.State.UNASSIGNED) {
        if (wasFailure) {
          failures += 1;
          taskStatus.setRunState(TaskStatus.State.FAILED);
        } else {
          taskStatus.setRunState(TaskStatus.State.KILLED);
        }
      }
      removeFromMemoryManager(task.getTaskID());
      if (maxHeapSizeBasedChecking && !isEphemeral()) {
        freeTaskMemory(getMemoryReserved());
      }
      releaseSlot();
      notifyTTAboutTaskCompletion(task.isMapTask(), true);
    }
    
    private synchronized void releaseSlot() {
      if (slotTaken) {
        if (launcher != null) {
          launcher.addFreeSlots(task.getNumSlotsRequired());
        }
        slotTaken = false;
      } else {
        // wake up the launcher. it may be waiting to block slots for this task.
        if (launcher != null) {
          launcher.notifySlots();
        }
      }
    }

    /**
     * The map output has been lost.
     */
    private synchronized void mapOutputLost(String failure
                                           ) throws IOException {
      if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING || 
          taskStatus.getRunState() == TaskStatus.State.SUCCEEDED) {
        // change status to failure
        LOG.info("Reporting output lost:"+task.getTaskID());
        taskStatus.setRunState(TaskStatus.State.FAILED);
        taskStatus.setProgress(0.0f);
        reportDiagnosticInfo("Map output lost, rescheduling: " + 
                             failure);
        runningTasks.put(task.getTaskID(), this);
        increaseMapTask(1);
      } else {
        LOG.warn("Output already reported lost:"+task.getTaskID());
      }
    }

    /**
     * We no longer need anything from this task.  Either the 
     * controlling job is all done and the files have been copied
     * away, or the task failed and we don't need the remains.
     * Any calls to cleanup should not lock the tip first.
     * cleanup does the right thing- updates tasks in Tasktracker
     * by locking tasktracker first and then locks the tip.
     * 
     * if needCleanup is true, the whole task directory is cleaned up.
     * otherwise the current working directory of the task 
     * i.e. &lt;taskid&gt;/work is cleaned up.
     */
    void cleanup(boolean needCleanup) throws IOException {
      TaskAttemptID taskId = task.getTaskID();

      if (LOG.isDebugEnabled())
        LOG.debug("Cleaning up task: " + taskId);

      synchronized (TaskTracker.this) {
        if (needCleanup) {
          // see if tasks data structure is holding this tip.
          // tasks could hold the tip for cleanup attempt, if cleanup attempt 
          // got launched before this method.
          if (tasks.get(taskId) == this) {
            tasks.remove(taskId);

          }
        }
        synchronized (this){
          if (alwaysKeepTaskFiles ||
              (taskStatus.getRunState() == TaskStatus.State.FAILED && 
               keepFailedTaskFiles)) {
            return;
          }
        }
      }
      synchronized (this) {
        // localJobConf could be null if localization has not happened
        // then no cleanup will be required.
        if (localJobConf == null) {
          return;
        }
        try {
          removeTaskFiles(needCleanup);
        } catch (Throwable ie) {
          LOG.info("Error cleaning up task runner: "
              + StringUtils.stringifyException(ie));
        }
      }
    }

    /**
     * Some or all of the files from this task are no longer required. Remove
     * them via CleanupQueue.
     * 
     * @param removeOutputs remove outputs as well as output
     * @param taskId
     * @throws IOException 
     */
    void removeTaskFiles(boolean removeOutputs) throws IOException {
      if (localJobConf.getNumTasksToExecutePerJvm() == 1) {
        String user = ugi.getShortUserName();
        int userDirLen = TaskTracker.getUserDir(user).length();
        String jobId = task.getJobID().toString();
        String taskId = task.getTaskID().toString();
        boolean cleanup = task.isTaskCleanupTask();
        String taskDir;
        if (!removeOutputs) {
          taskDir = TaskTracker.getTaskWorkDir(user, jobId, taskId, cleanup);
        } else {
          taskDir = TaskTracker.getLocalTaskDir(user, jobId, taskId, cleanup);
        }
        PathDeletionContext item =
          new TaskController.DeletionContext(taskController, false, user,
                                             taskDir.substring(userDirLen));          
        directoryCleanupThread.addToQueue(item);
      }
    }
        
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof TaskInProgress) &&
        task.getTaskID().equals
        (((TaskInProgress) obj).getTask().getTaskID());
    }
        
    @Override
    public int hashCode() {
      return task.getTaskID().hashCode();
    }
  }
 
  private void validateJVM(TaskInProgress tip, JvmContext jvmContext, TaskAttemptID taskid) throws IOException {
    if (jvmContext == null) {
      LOG.warn("Null jvmContext. Cannot verify Jvm. validateJvm throwing exception");
      throw new IOException("JvmValidate Failed. JvmContext is null - cannot validate JVM");
    }
    if (!jvmManager.validateTipToJvm(tip, jvmContext.jvmId) && !tip.isJvmValidationFailureLogged()) {
      tip.setJvmValidationFailureLogged();
      throw new IOException("JvmValidate Failed. Ignoring request from task: " + taskid + ", with JvmId: " + jvmContext.jvmId);
    }
  }

  /**
   * Check that the current UGI is the JVM authorized to report
   * for this particular job.
   *
   * @throws IOException for unauthorized access
   */
  private void ensureAuthorizedJVM(org.apache.hadoop.mapreduce.JobID jobId)
  throws IOException {
    String currentJobId = 
      UserGroupInformation.getCurrentUser().getUserName();
    if (!currentJobId.equals(jobId.toString())) {
      throw new IOException ("JVM with " + currentJobId + 
          " is not authorized for " + jobId);
    }
  }

    
  // ///////////////////////////////////////////////////////////////
  // TaskUmbilicalProtocol
  /////////////////////////////////////////////////////////////////

  /**
   * Called upon startup by the child process, to fetch Task data.
   */
  public synchronized JvmTask getTask(JvmContext context) 
  throws IOException {
    ensureAuthorizedJVM(context.jvmId.getJobId());
    JVMId jvmId = context.jvmId;
    LOG.debug("JVM with ID : " + jvmId + " asked for a task");
    // if this is the first time jvm is reporting pid then create a pid file
    if (jvmManager.getPid(jvmId) == null) {
      if (LOG.isInfoEnabled())
        LOG.info("Setting pid " + context.pid + " for jvm " + jvmId);
      // save pid of task JVM sent by child 
      jvmManager.setPidToJvm(jvmId, context.pid);
      createPidFile(jvmId, context.pid);
    }
    if (!jvmManager.isJvmKnown(jvmId)) {
      LOG.info("Killing unknown JVM " + jvmId);
      return new JvmTask(null, true);
    }
    RunningJob rjob = runningJobs.get(jvmId.getJobId());
    if (rjob == null) { //kill the JVM since the job is dead
      LOG.info("Killing JVM " + jvmId + " since job " + jvmId.getJobId() +
               " is dead");
      try {
        jvmManager.killJvm(jvmId);
      } catch (InterruptedException e) {
        LOG.warn("Failed to kill " + jvmId, e);
      }
      return new JvmTask(null, true);
    }
    TaskInProgress tip = jvmManager.getTaskForJvm(jvmId);
    if (tip == null) {
      return new JvmTask(null, false);
    }
    if (tasks.get(tip.getTask().getTaskID()) != null) { //is task still present
      LOG.info("JVM with ID: " + jvmId + " given task: " + 
          tip.getTask().getTaskID());
      return new JvmTask(tip.getTask(), false);
    } else {
      LOG.info("Killing JVM with ID: " + jvmId + " since scheduled task: " + 
          tip.getTask().getTaskID() + " is " + tip.taskStatus.getRunState());
      return new JvmTask(null, true);
    }
  }

  /**
   * Called periodically to report Task progress, from 0.0 to 1.0.
   */
  public synchronized boolean statusUpdate(TaskAttemptID taskid, 
                                              TaskStatus taskStatus,
                                              JvmContext jvmContext)
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      try {
        validateJVM(tip, jvmContext, taskid);
      } catch (IOException ie) {
        LOG.warn("Failed validating JVM", ie);
        return false;
      }
      tip.reportProgress(taskStatus);
      return true;
    } else {
      LOG.warn("Progress from unknown child task: "+taskid);
      return false;
    }
  }

  /**
   * Called when the task dies before completion, and we want to report back
   * diagnostic info
   */
  public synchronized void reportDiagnosticInfo(TaskAttemptID taskid,
      String info, JvmContext jvmContext) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskid);
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }

  /**
   * Same as reportDiagnosticInfo but does not authorize caller. This is used
   * internally within MapReduce, whereas reportDiagonsticInfo may be called
   * via RPC.
   */
  synchronized void internalReportDiagnosticInfo(TaskAttemptID taskid, String info) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDiagnosticInfo(info);
    } else {
      LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
    }
  }
  
  public synchronized void reportNextRecordRange(TaskAttemptID taskid, 
      SortedRanges.Range range, JvmContext jvmContext) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskid);
      tip.reportNextRecordRange(range);
    } else {
      LOG.warn("reportNextRecordRange from unknown child task: "+taskid+". " +
      		"Ignored.");
    }
  }

  /** Child checking to see if we're alive.  Normally does nothing.*/
  public synchronized boolean ping(TaskAttemptID taskid, JvmContext jvmContext)
      throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.updateLastPing();
      validateJVM(tip, jvmContext, taskid);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Task is reporting that it is in commit_pending
   * and it is waiting for the commit Response
   */
  public synchronized void commitPending(TaskAttemptID taskid,
                                         TaskStatus taskStatus,
                                         JvmContext jvmContext) 
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    LOG.info("Task " + taskid + " is in commit-pending," +"" +
             " task state:" +taskStatus.getRunState());
    // validateJVM is done in statusUpdate
    if (!statusUpdate(taskid, taskStatus, jvmContext)) {
      throw new IOException("Task not found for taskid: " + taskid);
    }
    reportTaskFinished(taskid, true);
  }
  
  /**
   * Child checking whether it can commit 
   */
  public synchronized boolean canCommit(TaskAttemptID taskid,
      JvmContext jvmContext) throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    validateJVM(tip, jvmContext, taskid);
    return commitResponses.contains(taskid); // don't remove it now
  }
  
  /**
   * The task is done.
   */
  public synchronized void done(TaskAttemptID taskid, JvmContext jvmContext) 
  throws IOException {
    ensureAuthorizedJVM(taskid.getJobID());
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskid);
      commitResponses.remove(taskid);
      tip.reportDone();
    } else {
      LOG.warn("Unknown child task done: "+taskid+". Ignored.");
    }
  }


  /** 
   * A reduce-task failed to shuffle the map-outputs. Kill the task.
   */  
  public synchronized void shuffleError(TaskAttemptID taskId, String message, JvmContext jvmContext) 
  throws IOException { 
    ensureAuthorizedJVM(taskId.getJobID());
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskId);
      LOG.fatal("Task: " + taskId + " - Killed due to Shuffle Failure: " 
                + message); 
      tip.reportDiagnosticInfo("Shuffle Error: " + message);
      purgeTask(tip, true, true);
    } else {
      LOG.warn("Unknown child task shuffleError: " + taskId + ". Ignored.");
    }
  }

  /** 
   * A child task had a local filesystem error. Kill the task.
   */  
  public synchronized void fsError(TaskAttemptID taskId, String message,
      JvmContext jvmContext) throws IOException {
    ensureAuthorizedJVM(taskId.getJobID());
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskId);
      LOG.fatal("Task: " + taskId + " - Killed due to FSError: " + message);
      tip.reportDiagnosticInfo("FSError: " + message);
      purgeTask(tip, true, true);
    } else {
      LOG.warn("Unknown child task fsError: "+taskId+". Ignored.");
    }
  }

  /**
   * Version of fsError() that does not do authorization checks, called by
   * the TaskRunner.
   */
  synchronized void internalFsError(TaskAttemptID taskId, String message)
  throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed due to FSError: " + message);
    TaskInProgress tip = runningTasks.get(taskId);
    tip.reportDiagnosticInfo("FSError: " + message);
    purgeTask(tip, true, true);
  }

  /** 
   * A child task had a fatal error. Kill the task.
   */  
  public synchronized void fatalError(TaskAttemptID taskId, String msg, 
      JvmContext jvmContext) throws IOException {
    ensureAuthorizedJVM(taskId.getJobID());
    TaskInProgress tip = runningTasks.get(taskId);
    if (tip != null) {
      validateJVM(tip, jvmContext, taskId);
      LOG.fatal("Task: " + taskId + " - Killed : " + msg);
      tip.reportDiagnosticInfo("Error: " + msg);
      purgeTask(tip, true, true);
    } else {
      LOG.warn("Unknown child task fatalError: "+taskId+". Ignored.");
    }
  }

  public synchronized MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobId, int fromEventId, int maxLocs, TaskAttemptID id,
      JvmContext jvmContext) throws IOException {
    TaskInProgress tip = runningTasks.get(id);
    if (tip == null) {
      throw new IOException("Unknown task; " + id
          + ". Ignoring getMapCompletionEvents Request");
    }
    validateJVM(tip, jvmContext, id);
    ensureAuthorizedJVM(jobId);
    TaskCompletionEvent[]mapEvents = TaskCompletionEvent.EMPTY_ARRAY;
    synchronized (shouldReset) {
      if (shouldReset.remove(id)) {
        return new MapTaskCompletionEventsUpdate(mapEvents, true);
      }
    }
    RunningJob rjob;
    synchronized (runningJobs) {
      rjob = runningJobs.get(jobId);          
      if (rjob != null) {
        synchronized (rjob) {
          FetchStatus f = rjob.getFetchStatus();
          if (f != null) {
            mapEvents = f.getMapEvents(fromEventId, maxLocs);
          }
        }
      }
    }
    return new MapTaskCompletionEventsUpdate(mapEvents, false);
  }
    
  /////////////////////////////////////////////////////
  //  Called by TaskTracker thread after task process ends
  /////////////////////////////////////////////////////
  /**
   * The task is no longer running.  It may not have completed successfully
   */
  void reportTaskFinished(TaskAttemptID taskid, boolean commitPending) {
    TaskInProgress tip;
    synchronized (this) {
      tip = tasks.get(taskid);
    }
    if (tip != null) {
      tip.reportTaskFinished(commitPending);
    } else {
      LOG.warn("Unknown child task finished: "+taskid+". Ignored.");
    }
  }
  

  /**
   * A completed map task's output has been lost.
   */
  public synchronized void mapOutputLost(TaskAttemptID taskid,
                                         String errorMsg) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.mapOutputLost(errorMsg);
    } else {
      LOG.warn("Unknown child with bad map output: "+taskid+". Ignored.");
    }
  }
  
 /** Check if TT is asked to resend info */  
  private boolean resendTaskReportsAction(TaskTrackerAction[] actions) {
    if (actions != null) {
      for (TaskTrackerAction action : actions) {
        if (action.getActionId() == 
            TaskTrackerAction.ActionType.RESEND_STATUS) {
          LOG.info("Received ResendTaskReportsAction from JobTracker");
          return true;
        }
      }
    }
    return false;
  }

  private void killIdleJvms() throws IOException {
    if (maxJvmIdleTime > 0)
      jvmManager.killIdleJvms(maxJvmIdleTime);
  }

  /**
   *  The datastructure for initializing a job
   */
  static class RunningJob{
    private JobID jobid; 
    private JobConf jobConf;
    private Path localizedJobConf;
    // keep this for later use
    volatile Set<TaskInProgress> tasks;
    //the 'localizing' and 'localized' fields have the following
    //state transitions (first entry is for 'localizing')
    //{false,false} -> {true,false} -> {false,true}
    volatile boolean localized;
    boolean localizing;
    boolean keepJobFiles;
    volatile boolean isKilled = false;
    UserGroupInformation ugi;
    FetchStatus f;
    TaskDistributedCacheManager distCacheMgr;

    RunningJob(JobID jobid) {
      this.jobid = jobid;
      localized = false;
      localizing = false;
      tasks = new HashSet<TaskInProgress>();
      keepJobFiles = false;
      isKilled  = false;
    }
      
    JobID getJobID() {
      return jobid;
    }
      
    UserGroupInformation getUGI() {
      return ugi;
    }

    void setFetchStatus(FetchStatus f) {
      this.f = f;
    }
      
    FetchStatus getFetchStatus() {
      return f;
    }

    JobConf getJobConf() {
      return jobConf;
    }

    void markKilled() {
      this.isKilled = true;
    }

    boolean isKilled() {
      return this.isKilled;
    }
  }

  /**
   * Get the name for this task tracker.
   * @return the string like "tracker_mymachine:50010"
   */
  String getName() {
    return taskTrackerName;
  }
    
  private synchronized List<TaskStatus> cloneAndResetRunningTaskStatuses(
                                          boolean sendCounters) {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip: runningTasks.values()) {
      TaskStatus status = tip.getStatus();
      status.setIncludeCounters(sendCounters);
      // send counters for finished or failed tasks and commit pending tasks
      if (status.getRunState() != TaskStatus.State.RUNNING) {
        status.setIncludeCounters(true);
      }
      result.add((TaskStatus)status.clone());
      status.clearStatus();
    }
    return result;
  }
  
  /* Count running map tasks */
  private synchronized int getRunningMapTasksCount() {
    int count = 0;
    for(TaskInProgress tip: runningTasks.values()) {
      TaskStatus status = tip.getStatus();
      if (status.getRunState() == TaskStatus.State.RUNNING) {
        if (tip.getTask().isMapTask()) {
          count++;
        }
      }
    }
    return count;
  }

  /* Count running reduce tasks */
  private synchronized int getRunningReduceTasksCount() {
    int count = 0;
    for(TaskInProgress tip: runningTasks.values()) {
      TaskStatus status = tip.getStatus();
      if (status.getRunState() == TaskStatus.State.RUNNING) {
        if (!tip.getTask().isMapTask()) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Get the list of tasks that will be reported back to the 
   * job tracker in the next heartbeat cycle.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getRunningTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for(TaskInProgress tip: runningTasks.values()) {
      result.add(tip.getStatus());
    }
    return result;
  }

  /* Get status for all task on this TT, used for recovery */
  private synchronized List<TaskStatus> cloneAllTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for (Map.Entry<TaskAttemptID, TaskInProgress> task: tasks.entrySet()) {
      TaskStatus status = task.getValue().getStatus();
      status.setIncludeCounters(true);
      result.add((TaskStatus)status.clone());
      if (runningTasks.containsKey(task.getKey())) {
        status.clearStatus();
      }
    }
    return result;
  }

  /**
   * Get the list of stored tasks on this task tracker.
   * @return
   */
  synchronized List<TaskStatus> getNonRunningTasks() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for(Map.Entry<TaskAttemptID, TaskInProgress> task: tasks.entrySet()) {
      if (!runningTasks.containsKey(task.getKey())) {
        result.add(task.getValue().getStatus());
      }
    }
    return result;
  }


  /**
   * Get the list of tasks from running jobs on this task tracker.
   * @return a copy of the list of TaskStatus objects
   */
  synchronized List<TaskStatus> getTasksFromRunningJobs() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(tasks.size());
    for (Map.Entry <JobID, RunningJob> item : runningJobs.entrySet()) {
      RunningJob rjob = item.getValue();
      synchronized (rjob) {
        for (TaskInProgress tip : rjob.tasks) {
          result.add(tip.getStatus());
        }
      }
    }
    return result;
  }
  
  /**
   * Get the default job conf for this tracker.
   */
  JobConf getJobConf() {
    return fConf;
  }
    
  /**
   * Check if the given local directories
   * (and parent directories, if necessary) can be created.
   * @param localDirs where the new TaskTracker should keep its local files.
   * @param checkAndFixPermissions should check the permissions of the directory
   *        and try to fix them if incorrect. This is expensive so should only be
   *        done at startup.
   * @throws DiskErrorException if all local directories are not writable
   */
  private static void checkLocalDirs(LocalFileSystem localFs, 
                                     String[] localDirs,
                                     boolean checkAndFixPermissions) 
    throws DiskErrorException {
    boolean writable = false;
        
    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          if (checkAndFixPermissions) {
            DiskChecker.checkDir(localFs, new Path(localDirs[i]),
                                 LOCAL_DIR_PERMISSION);
          } else {
            DiskChecker.checkDir(new File(localDirs[i]));
          }

          writable = true;
        } catch(IOException e) {
          LOG.warn("Task Tracker local " + e.getMessage());
        }
      }
    }

    if (!writable)
      throw new DiskErrorException("all local directories are not writable");
  }
    
  /**
   * Is this task tracker idle?
   * @return has this task tracker finished and cleaned up all of its tasks?
   */
  public synchronized boolean isIdle() {
    return tasks.isEmpty() && tasksToCleanup.isEmpty();
  }
 
  public static void usage() {
    System.out.println("usage: TaskTracker [-Dtt.tasks.mem=4000]");
    System.exit(-1);
  }
    
  /**
   * Start the TaskTracker, point toward the indicated JobTracker
   */
  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(TaskTracker.class, argv, LOG);
    StringBuffer b = new StringBuffer();
    b.append("\n/*-------------- TaskTracker System Properties ----------------");
    Properties p = System.getProperties();
    Enumeration keys = p.keys();
    while (keys.hasMoreElements()) {
      String key = (String)keys.nextElement();
      String value = (String)p.get(key);
      b.append("\n" + key + ": " + value);
    }       
    b.append("\nrpc.version: " + Server.CURRENT_VERSION);
    b.append("\n------------------------------------------------------------*/");
    LOG.info(b.toString());

    if (argv.length != 0) {
      usage();
    }
    try {

      // enable the server to track time spent waiting on locks
      ReflectionUtils.setContentionTracing
        (DEFAULT_CONF.getBoolean("tasktracker.contention.tracking", false));
      TaskTracker tt = new TaskTracker(DEFAULT_CONF);
      MBeanUtil.registerMBean("TaskTracker", "TaskTrackerInfo", tt);
      tt.run();
    } catch (Throwable e) {
      LOG.error("Can not start TaskTracker because "+
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
    System.exit(0);
  }

  static class LRUCache<K, V> {
    private int cacheSize;
    private LinkedHashMap<K, V> map;

    public LRUCache(int cacheSize) {
      this.cacheSize = cacheSize;
      this.map = new LinkedHashMap<K, V>(cacheSize, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
          return size() > LRUCache.this.cacheSize;
        }
      };
    }

    public synchronized V get(K key) {
      return map.get(key);
    }

    public synchronized void put(K key, V value) {
      map.put(key, value);
    }
 
    public synchronized int size() {
      return map.size();
    }

    public Iterator<Entry<K, V>> getIterator() {
      return new LinkedList<Entry<K, V>>(map.entrySet()).iterator();
    }

    public synchronized void clear() {
      map.clear();
    }
  }

  /**
   * This class is used in TaskTracker's Jetty to serve the map outputs
   * to other nodes.
   */
  public static class MapOutputServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final int MAX_BYTES_TO_READ = 64 * 1024;
    
    private static LRUCache<String, Path> fileCache = new LRUCache<String, Path>(FILE_CACHE_SIZE);
    private static LRUCache<String, Path> fileIndexCache = new LRUCache<String, Path>(FILE_CACHE_SIZE);
    
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      String mapId = request.getParameter("map");
      String reduceId = request.getParameter("reduce");
      String jobId = request.getParameter("job");

      if (jobId == null) {
        throw new IOException("job parameter is required");
      }

      if (mapId == null || reduceId == null) {
        throw new IOException("map and reduce parameters are required");
      }
      ServletContext context = getServletContext();
      int reduce = Integer.parseInt(reduceId);
      byte[] buffer = new byte[MAX_BYTES_TO_READ];
      // true iff IOException was caused by attempt to access input
      boolean isInputException = true;
      OutputStream outStream = null;
      FileInputStream mapOutputIn = null;
 
      long totalRead = 0;
      ShuffleServerMetrics shuffleMetrics =
        (ShuffleServerMetrics) context.getAttribute("shuffleServerMetrics");
      TaskTracker tracker = 
        (TaskTracker) context.getAttribute("task.tracker");

      verifyRequest(request, response, tracker, jobId);

      long startTime = 0;
      try {
        shuffleMetrics.serverHandlerBusy();
        if(ClientTraceLog.isInfoEnabled())
          startTime = System.nanoTime();
        response.setContentType("application/octet-stream");
        outStream = response.getOutputStream();
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc = 
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem rfs = ((LocalFileSystem)
            context.getAttribute("local.file.system")).getRaw();

      String userName = null;
      String runAsUserName = null;
      synchronized (tracker.runningJobs) {
        RunningJob rjob = tracker.runningJobs.get(JobID.forName(jobId));
        if (rjob == null) {
          throw new IOException("Unknown job " + jobId + "!!");
        }
        userName = rjob.jobConf.getUser();
        runAsUserName = tracker.getTaskController().getRunAsUser(rjob.jobConf);
      }
      // Index file
      String intermediateOutputDir = TaskTracker.getIntermediateOutputDir(userName, jobId, mapId);
      String indexKey = intermediateOutputDir + "/file.out.index";
      Path indexFileName = fileIndexCache.get(indexKey);
      if (indexFileName == null) {
        indexFileName = lDirAlloc.getLocalPathToRead(indexKey, conf);
        fileIndexCache.put(indexKey, indexFileName);
      }

      // Map-output file
      String fileKey = intermediateOutputDir + "/file.out";
      Path mapOutputFileName = fileCache.get(fileKey);
      if (mapOutputFileName == null) {
        mapOutputFileName = lDirAlloc.getLocalPathToRead(fileKey, conf);
        fileCache.put(fileKey, mapOutputFileName);
      }

        /**
         * Read the index file to get the information about where
         * the map-output for the given reducer is available. 
         */
        IndexRecord info = 
          tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName, 
              runAsUserName);
          
        //set the custom "from-map-task" http header to the map task from which
        //the map output data is being transferred
        response.setHeader(FROM_MAP_TASK, mapId);
        
        //set the custom "Raw-Map-Output-Length" http header to 
        //the raw (decompressed) length
        response.setHeader(RAW_MAP_OUTPUT_LENGTH,
            Long.toString(info.rawLength));

        //set the custom "Map-Output-Length" http header to 
        //the actual number of bytes being transferred
        response.setHeader(MAP_OUTPUT_LENGTH,
            Long.toString(info.partLength));

        //set the custom "for-reduce-task" http header to the reduce task number
        //for which this map output is being transferred
        response.setHeader(FOR_REDUCE_TASK, Integer.toString(reduce));
        
        //use the same buffersize as used for reading the data from disk
        response.setBufferSize(MAX_BYTES_TO_READ);
        
        /**
         * Read the data from the sigle map-output file and
         * send it to the reducer.
         */
        //open the map-output file
        mapOutputIn = SecureIOUtils.openForRead(
            new File(mapOutputFileName.toUri().getPath()), runAsUserName);

        //seek to the correct offset for the reduce
        mapOutputIn.skip(info.startOffset);
        long rem = info.partLength;
        int len =
          mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        while (rem > 0 && len >= 0) {
          rem -= len;
          try {
            shuffleMetrics.outputBytes(len);
            outStream.write(buffer, 0, len);
            outStream.flush();
          } catch (IOException ie) {
            isInputException = false;
            throw ie;
          }
          totalRead += len;
          len =
            mapOutputIn.read(buffer, 0, (int)Math.min(rem, MAX_BYTES_TO_READ));
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.info("Sent out " + totalRead + " bytes for reduce: " + reduce + 
                 " from map: " + mapId + " given " + info.partLength + "/" + 
                 info.rawLength);
        }
      } catch (IOException ie) {
        Log log = (Log) context.getAttribute("log");
        String errorMsg = ("getMapOutput(" + mapId + "," + reduceId + 
                           ") failed : " +
                           StringUtils.stringifyException(ie));
        log.warn(errorMsg);
        if (isInputException) {
          tracker.mapOutputLost(TaskAttemptID.forName(mapId), errorMsg);
        }
        response.sendError(HttpServletResponse.SC_GONE, errorMsg);
        shuffleMetrics.failedOutput();
        throw ie;
      } finally {
        if (null != mapOutputIn) {
          mapOutputIn.close();
        }
        final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
        shuffleMetrics.serverHandlerFree();
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
                request.getLocalAddr() + ":" + request.getLocalPort(),
                request.getRemoteAddr() + ":" + request.getRemotePort(),
                totalRead, "MAPRED_SHUFFLE", mapId, endTime-startTime));
        }
      }
      outStream.close();
      shuffleMetrics.successOutput();
    }
    
    /**
     * verify that request has correct HASH for the url
     * and also add a field to reply header with hash of the HASH
     * @param request
     * @param response
     * @param jt the job token
     * @throws IOException
     */
    private void verifyRequest(HttpServletRequest request, 
        HttpServletResponse response, TaskTracker tracker, String jobId) 
    throws IOException {
      SecretKey tokenSecret = tracker.getJobTokenSecretManager()
          .retrieveTokenSecret(jobId);
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(request);
      
      // hash from the fetcher
      String urlHashStr = request.getHeader(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if(urlHashStr == null) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        throw new IOException("fetcher cannot be authenticated " + 
            request.getRemoteHost());
      }
      int len = urlHashStr.length();
      LOG.debug("verifying request. enc_str="+enc_str+"; hash=..."+
          urlHashStr.substring(len-len/2, len-1)); // half of the hash for debug

      // verify - throws exception
      try {
        SecureShuffleUtils.verifyReply(urlHashStr, enc_str, tokenSecret);
      } catch (IOException ioe) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        throw ioe;
      }
      
      // verification passed - encode the reply
      String reply = SecureShuffleUtils.generateHash(urlHashStr.getBytes(), tokenSecret);
      response.addHeader(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
      
      len = reply.length();
      LOG.debug("Fetcher request verfied. enc_str="+enc_str+";reply="
          +reply.substring(len-len/2, len-1));
    }
  }
  

  // get the full paths of the directory in all the local disks.
  Path[] getLocalFiles(JobConf conf, String subdir) throws IOException{
    String[] localDirs = conf.getLocalDirs();
    Path[] paths = new Path[localDirs.length];
    FileSystem localFs = FileSystem.getLocal(conf);
    boolean subdirNeeded = (subdir != null) && (subdir.length() > 0);
    for (int i = 0; i < localDirs.length; i++) {
      paths[i] = (subdirNeeded) ? new Path(localDirs[i], subdir)
                                : new Path(localDirs[i]);
      paths[i] = paths[i].makeQualified(localFs);
    }
    return paths;
  }

  FileSystem getLocalFileSystem(){
    return localFs;
  }

  // only used by tests
  void setLocalFileSystem(FileSystem fs){
    localFs = (LocalFileSystem)fs;
  }

  int getMaxCurrentMapTasks() {
    return maxMapSlots;
  }
  
  int getMaxCurrentReduceTasks() {
    return maxReduceSlots;
  }

  int getEphemeralSlots() {
    return maxEphemeralSlots;
  }

  //called from unit test
  synchronized void setMaxMapSlots(int mapSlots) {
    maxMapSlots = mapSlots;
  }

  //called from unit test
  synchronized void setMaxReduceSlots(int reduceSlots) {
    maxReduceSlots = reduceSlots;
  }

  /**
   * Is the TaskMemoryManager Enabled on this system?
   * @return true if enabled, false otherwise.
   */
  public boolean isTaskMemoryManagerEnabled() {
    return taskMemoryManagerEnabled;
  }
  
  public TaskMemoryManagerThread getTaskMemoryManager() {
    return taskMemoryManager;
  }

  /**
   * Normalize the negative values in configuration
   * 
   * @param val
   * @return normalized val
   */
  private long normalizeMemoryConfigValue(long val) {
    if (val < 0) {
      val = JobConf.DISABLED_MEMORY_LIMIT;
    }
    return val;
  }

  /**
   * Memory-related setup
   */
  private void initializeMemoryManagement() {

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_VMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          MAPRED_TASKTRACKER_PMEM_RESERVED_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY));
    }

    //handling @deprecated
    if (fConf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY));
    }
    // Use TT_MEMORY_CALCULATOR_PLUGIN if it is configured.
    Class<? extends MemoryCalculatorPlugin> clazz =
      fConf.getClass(MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
          null, MemoryCalculatorPlugin.class);
    MemoryCalculatorPlugin memoryCalculatorPlugin = (clazz == null ?
        null : MemoryCalculatorPlugin.getMemoryCalculatorPlugin(clazz, fConf));
    if (memoryCalculatorPlugin != null || resourceCalculatorPlugin != null) {
      totalVirtualMemoryOnTT = (memoryCalculatorPlugin == null ?
          resourceCalculatorPlugin.getVirtualMemorySize() :
          memoryCalculatorPlugin.getVirtualMemorySize());
      if (totalVirtualMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalVmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalVirtualMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
      totalPhysicalMemoryOnTT = (memoryCalculatorPlugin == null ?
          resourceCalculatorPlugin.getPhysicalMemorySize() :
          memoryCalculatorPlugin.getPhysicalMemorySize());
      if (totalPhysicalMemoryOnTT <= 0) {
        LOG.warn("TaskTracker's totalPmem could not be calculated. "
            + "Setting it to " + JobConf.DISABLED_MEMORY_LIMIT);
        totalPhysicalMemoryOnTT = JobConf.DISABLED_MEMORY_LIMIT;
      }
    }
   
    LOG.info("Total Virtual Memory = " + totalVirtualMemoryOnTT);
    LOG.info("Total Physical Memory = " + totalPhysicalMemoryOnTT);
    mapSlotMemorySizeOnTT =
      fConf.getLong(
          JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
          JobConf.DISABLED_MEMORY_LIMIT);
    reduceSlotSizeMemoryOnTT =
      fConf.getLong(
          JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
          JobConf.DISABLED_MEMORY_LIMIT);
    
    totalMemoryAllottedForTasks =
        maxMapSlots * mapSlotMemorySizeOnTT + maxReduceSlots
            * reduceSlotSizeMemoryOnTT;    
    if (totalMemoryAllottedForTasks < 0) 
      totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;

    LOG.info("Virtual memory limit per map slot = " + mapSlotMemorySizeOnTT);
    LOG.info("Virtual memory limit per reduce slot = " + reduceSlotSizeMemoryOnTT);
    LOG.info("Virtual memory limit for mapreduce tasks = " + totalMemoryAllottedForTasks);

    if (totalMemoryAllottedForTasks < 0) {
      //adding check for the old keys which might be used by the administrator
      //while configuration of the memory monitoring on TT
      long memoryAllotedForSlot = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY, 
              JobConf.DISABLED_MEMORY_LIMIT));
      long limitVmPerTask = fConf.normalizeMemoryConfigValue(
          fConf.getLong(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, 
              JobConf.DISABLED_MEMORY_LIMIT));
      if(memoryAllotedForSlot == JobConf.DISABLED_MEMORY_LIMIT) {
        totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT; 
      } else {
        if(memoryAllotedForSlot > limitVmPerTask) {
          LOG.info("DefaultMaxVmPerTask is mis-configured. " +
          		"It shouldn't be greater than task limits");
          totalMemoryAllottedForTasks = JobConf.DISABLED_MEMORY_LIMIT;
        } else {
          totalMemoryAllottedForTasks = (maxMapSlots + 
              maxReduceSlots) *  (memoryAllotedForSlot/(1024 * 1024));
        }
      }
    }

    if (totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT) {
      LOG.warn("totalMemoryAllottedForTasks > totalPhysicalMemoryOnTT."
          + " Thrashing might happen.");
    } else if (totalMemoryAllottedForTasks > totalVirtualMemoryOnTT) {
      LOG.warn("totalMemoryAllottedForTasks > totalVirtualMemoryOnTT."
          + " Thrashing might happen.");
    }

    // start the taskMemoryManager thread only if enabled
    setTaskMemoryManagerEnabledFlag();
    if (isTaskMemoryManagerEnabled()) {
      taskMemoryManager = new TaskMemoryManagerThread(this);
      taskMemoryManager.setDaemon(true);
      taskMemoryManager.start();
    }
  }

  void setTaskMemoryManagerEnabledFlag() {
    if (!ProcfsBasedProcessTree.isAvailable()) {
      LOG.info("ProcessTree implementation is missing on this system. " +
               "TaskMemoryManager is disabled.");
      taskMemoryManagerEnabled = false;
      return;
    }

    if (reservedPhysicalMemoryOnTT == JobConf.DISABLED_MEMORY_LIMIT &&
        totalMemoryAllottedForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      taskMemoryManagerEnabled = false;
      LOG.warn("TaskTracker's totalMemoryAllottedForTasks is -1. and " +
               "reserved physical memory is not configured. " +
               " TaskMemoryManager is disabled.");
      return;
    }

    taskMemoryManagerEnabled = true;
  }

  /** Low memory detected. To avoid swapping we will kill
    tasks soon. Before that cancel all tasks in queue, give
    them back to jobtracker. 
    TT goes in degraded mode.
   */ 
  synchronized void lowMemoryDetected() {
    // cancel all unassiged tasks in queue
    for (TaskInProgress tip: runningTasks.values()) {
      if (tip != null && tip.getRunState() == TaskStatus.State.UNASSIGNED) {
        tip.abort();
      }
    }
    /* If node is already running out of memory, reset slots to 1 */
    if (this.isRunningWithLowMemory == true) {
      resetCurrentMapSlots(1);
      resetCurrentReduceSlots(1);
    }
    this.isRunningWithLowMemory = true;
    this.lastSlotUpdateTime = System.currentTimeMillis();
  }

  /** 
   * Max Slots varies depending upon memory usage on this box 
   * Assumes TT is locked by caller  
   */
  int getCurrentMaxMapSlots() {
    return this.isRunningWithLowMemory ? currentMaxMapSlots : maxMapSlots;
  }

  int getCurrentMaxReduceSlots() {
    return this.isRunningWithLowMemory ? currentMaxReduceSlots : maxReduceSlots;
  }

  int getCurrentMaxPrefetchSlots() {
    return this.isRunningWithLowMemory ? 0 : maxMapPrefetch; 
  }

  void resetCurrentMapSlots(int slots) {
    currentMaxMapSlots = slots;
  }

  void resetCurrentReduceSlots(int slots) {
    currentMaxReduceSlots = slots;
  }

  void incrCurrentMapSlots() {
    currentMaxMapSlots++;
    if (currentMaxMapSlots > maxMapSlots)
      currentMaxMapSlots = maxMapSlots;
  }

  void incrCurrentReduceSlots() {
    currentMaxReduceSlots++;
    if (currentMaxReduceSlots > maxReduceSlots)
      currentMaxReduceSlots = maxReduceSlots;
  }

  /**
   * Clean-up the task that TaskMemoryMangerThread requests to do so.
   * @param tid
   * @param wasFailure mark the task as failed or killed. 'failed' if true,
   *          'killed' otherwise
   * @param diagnosticMsg
   */
  synchronized void cleanUpOverMemoryTask(TaskAttemptID tid, boolean wasFailure,
      String diagnosticMsg) {
    TaskInProgress tip = runningTasks.get(tid);
    if (tip != null) {
      tip.reportDiagnosticInfo(diagnosticMsg);
      // Marking it as failed/killed, kill ASAP.
      try {
        purgeTask(tip, wasFailure, true); 
      } catch (IOException ioe) {
        LOG.warn("Couldn't purge the task of " + tid + ". Error : " + ioe);
      }
    }
  }
  
  /**
   * Wrapper method used by TaskTracker to check if {@link  NodeHealthCheckerService}
   * can be started
   * @param conf configuration used to check if service can be started
   * @return true if service can be started
   */
  private boolean shouldStartHealthMonitor(Configuration conf) {
    return NodeHealthCheckerService.shouldRun(conf);
  }
  
  /**
   * Wrapper method used to start {@link NodeHealthCheckerService} for 
   * Task Tracker
   * @param conf Configuration used by the service.
   */
  private void startHealthMonitor(Configuration conf) {
    healthChecker = new NodeHealthCheckerService(conf);
    healthChecker.start();
  }
  
  TrackerDistributedCacheManager getTrackerDistributedCacheManager() {
    return distributedCacheManager;
  }

    /**
     * Download the job-token file from the FS and save on local fs.
     * @param user
     * @param jobId
     * @return the local file system path of the downloaded file.
     * @throws IOException
     */
  private String localizeJobTokenFile(String user, JobID jobId)
        throws IOException {
      // check if the tokenJob file is there..
      Path skPath = new Path(systemDirectory, 
          jobId.toString()+"/"+TokenCache.JOB_TOKEN_HDFS_FILE);
      
      FileStatus status = null;
      long jobTokenSize = -1;
      status = systemFS.getFileStatus(skPath); //throws FileNotFoundException
      jobTokenSize = status.getLen();
      
      Path localJobTokenFile =
          lDirAlloc.getLocalPathForWrite(getPrivateDirJobTokenFile(user, 
              jobId.toString()), jobTokenSize, fConf);
    
      String localJobTokenFileStr = localJobTokenFile.toUri().getPath();
      if(LOG.isDebugEnabled())
        LOG.debug("localizingJobTokenFile from sd="+skPath.toUri().getPath() + 
            " to " + localJobTokenFileStr);
      
      // Download job_token
      systemFS.copyToLocalFile(skPath, localJobTokenFile);      
      return localJobTokenFileStr;
    }

    private String localizeUserJobTicketFile(String user, JobID jobId, JobConf localJobConf)
      throws IOException {
      // check if the userticket file is there..
      Path skPath = new Path(systemDirectory, 
          jobId.toString() + "/" + USER_TICKET_FILE);
      
      FileStatus status = null;
      long jobTokenSize = -1;
      
      try {
        status = systemFS.getFileStatus(skPath); //throws FileNotFoundException
        jobTokenSize = status.getLen();
      } catch (FileNotFoundException e) {
        LOG.error("UserTicketFile is not available", e);
        return null;
      }
      
      Path localUserTicketJobFile =
          lDirAlloc.getLocalPathForWrite(getPrivateDirUserTicketFile(user, 
              jobId.toString()), jobTokenSize, fConf);
      
      String localUserTicketJobFileStr = localUserTicketJobFile.toUri().getPath();
      if(LOG.isDebugEnabled())
        LOG.debug("localizingUserTicketJobFile from sd="+skPath.toUri().getPath() + 
            " to " + localUserTicketJobFileStr);
      
      // Download user ticket
      systemFS.copyToLocalFile(skPath, localUserTicketJobFile); 
      
      return localUserTicketJobFileStr;
    }

    JobACLsManager getJobACLsManager() {
      return aclsManager.getJobACLsManager();
    }
    
    ACLsManager getACLsManager() {
      return aclsManager;
    }

  // Begin MXBean implementation
  @Override
  public String getHostname() {
    return LOCALHOSTNAME;
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() +", r"+ VersionInfo.getRevision();
  }

  @Override
  public String getConfigVersion() {
    return originalConf.get(CONF_VERSION_KEY, CONF_VERSION_DEFAULT);
  }

  @Override
  public String getJobTrackerUrl() {
    return originalConf.get("mapred.job.tracker");
  }

  @Override
  public int getRpcPort() {
    return taskReportAddress.getPort();
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  @Override
  public boolean isHealthy() {
    boolean healthy = true;
    TaskTrackerHealthStatus hs = new TaskTrackerHealthStatus();
    if (healthChecker != null) {
      healthChecker.setHealthStatus(hs);
      healthy = hs.isNodeHealthy();
    }    
    return healthy;
  }

  @Override
  public String getTasksInfoJson() {
    return getTasksInfo().toJson();
  }

  InfoMap getTasksInfo() {
    InfoMap map = new InfoMap();
    int failed = 0;
    int commitPending = 0;
    for (TaskStatus st : getNonRunningTasks()) {
      if (st.getRunState() == TaskStatus.State.FAILED ||
          st.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        ++failed;
      } else if (st.getRunState() == TaskStatus.State.COMMIT_PENDING) {
        ++commitPending;
      }
    }
    map.put("running", runningTasks.size());
    map.put("failed", failed);
    map.put("commit_pending", commitPending);
    return map;
  }
  // End MXBean implemenation

  @Override
  public void 
  updatePrivateDistributedCacheSizes(org.apache.hadoop.mapreduce.JobID jobId,
                                     long[] sizes
                                     ) throws IOException {
    ensureAuthorizedJVM(jobId);
    distributedCacheManager.setArchiveSizes(jobId, sizes);
  }

  boolean isMapRDiagnosticsEnabled() {
    return fConf.getBoolean(
      "mapr.task.diagnostics.enabled",
      false);
  }
}
