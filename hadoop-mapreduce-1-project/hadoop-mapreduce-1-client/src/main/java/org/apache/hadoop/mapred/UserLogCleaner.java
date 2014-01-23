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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.*;

/**
 * This is used only in UserLogManager, to manage cleanup of user logs.
 */
public class UserLogCleaner extends Thread {
  private static final Log LOG = LogFactory.getLog(UserLogCleaner.class);
  static final String USERLOGCLEANUP_SLEEPTIME = 
    "mapreduce.tasktracker.userlogcleanup.sleeptime";
  static final int DEFAULT_USER_LOG_RETAIN_HOURS = 24; // 1 day
  static final int DEFAULT_USER_LOG_RETAIN_HOURS_MAX = 24 * 7; // 1 week
  static final long DEFAULT_THREAD_SLEEP_TIME = 1000 * 60 * 60; // 1 hour

  private UserLogManager userLogManager;
  private Map<JobID, Long> completedJobs = Collections
      .synchronizedMap(new HashMap<JobID, Long>());
  private final long threadSleepTime;
  private CleanupQueue cleanupQueue;

  private Clock clock;
  private FileSystem localFs;

  public UserLogCleaner(UserLogManager userLogManager, Configuration conf)
      throws IOException {
    super("UserLogCleanerThread");
    this.userLogManager = userLogManager;
    threadSleepTime = conf.getLong(USERLOGCLEANUP_SLEEPTIME,
        DEFAULT_THREAD_SLEEP_TIME);
    cleanupQueue = CleanupQueue.getInstance();
    localFs = FileSystem.getLocal(conf);
    setClock(new Clock());
    setDaemon(true);
  }

  void setClock(Clock clock) {
    this.clock = clock;
  }

  Clock getClock() {
    return this.clock;
  }

  CleanupQueue getCleanupQueue() {
    return cleanupQueue;
  }

  void setCleanupQueue(CleanupQueue cleanupQueue) {
    this.cleanupQueue = cleanupQueue;
  }

  @Override
  public void run() {
    LOG.info("Started " + getName() + " with sleep interval " + (threadSleepTime/(1000*60)) + " minutes");
    // This thread wakes up after every threadSleepTime interval
    // and deletes if there are any old logs.
    while (true) {
      try {
        // sleep
        Thread.sleep(threadSleepTime);
        processCompletedJobs();
      } catch (Throwable e) {
        LOG.warn(getClass().getSimpleName()
            + " encountered an exception while monitoring :", e);
        LOG.info("Ingoring the exception and continuing monitoring.");
      }
    }
  }

  void processCompletedJobs() throws IOException {
    long now = clock.getTime();
    int oldJobsCount = 0;
    // iterate through completedJobs and remove old logs.
    synchronized (completedJobs) {
      Iterator<Entry<JobID, Long>> completedJobIter = completedJobs.entrySet()
          .iterator();
      while (completedJobIter.hasNext()) {
        Entry<JobID, Long> entry = completedJobIter.next();
        // see if the job is old enough
        if (entry.getValue().longValue() <= now) {
          // add the job for deletion
          userLogManager.addLogEvent(new DeleteJobEvent(entry.getKey()));
          completedJobIter.remove();
          oldJobsCount++;
        }
      }
    }
    // this message would be logged once per hour (default)
    LOG.info("User logs for " + oldJobsCount + " jobs are marked for deletion");
  }

  public void deleteJobLogs(JobID jobid) throws IOException {
    deleteLogPath(jobid.toString());
  }

  private void monitorOldUserLogs(Configuration conf, String logDir, long modificationTime)
    throws IOException
  {
    // add all the log dirs to taskLogsMnonitor.
    JobID jobid = null;
    try {
      jobid = JobID.forName(logDir);
    } catch (IllegalArgumentException ie) {
      deleteLogPath(logDir);
      return;
    }
    // add the job log directory for deletion with default retain hours,
    // if it is not already added
    if (!completedJobs.containsKey(jobid)) {
      JobCompletedEvent jce =
        new JobCompletedEvent(jobid, modificationTime, getUserlogRetainHours(conf));
      userLogManager.addLogEvent(jce);
    }
  }

  /**
   * Clears all the logs in userlog directory.
   * 
   * Adds the job directories for deletion with default retain hours. Deletes
   * all other directories, if any. This is usually called on reinit/restart of
   * the TaskTracker
   * 
   * @param conf
   * @throws IOException
   */
  public void clearOldUserLogs(Configuration conf) throws IOException {
    int numLocalUserLogDirs = 0;
    int numCentralUserLogDirs = 0;
    File userLogDir = TaskLog.getUserLogDir();
    if (userLogDir.exists()) {
      for (File logDir : userLogDir.listFiles()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found dir: " + logDir.getAbsolutePath() + " to be monitored for deletion." 
            + " Last modified time: " + logDir.lastModified());
        }
        monitorOldUserLogs(conf, logDir.getName(), logDir.lastModified());
        numLocalUserLogDirs++;
      }
    }

    if (numLocalUserLogDirs > 0) {
      LOG.info(numLocalUserLogDirs + " directories under " + userLogDir.getAbsolutePath()
        + " are scheduled for deletion");
    }

    for (FileStatus logDir : CentralTaskLogUtil.getOldUserLogs()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found dir: " + logDir.getPath() + " to be monitored for deletion." 
          + " Last modified time: " + logDir.getModificationTime());
      }
      monitorOldUserLogs(conf, logDir.getPath().getName(), logDir.getModificationTime());
      numCentralUserLogDirs++;
    }

    if (numCentralUserLogDirs > 0) {
      LOG.info(numCentralUserLogDirs + " directories under " +
        CentralTaskLogUtil.USERLOG_ROOT_PATH.toString() + " are scheduled for deletion");
    }
  }

  /**
   * If the configuration is null or user-log retain hours is not configured,
   * the retain hours are {@value UserLogCleaner#DEFAULT_USER_LOG_RETAIN_HOURS}
   */
  static int getUserlogRetainHours(Configuration conf) {
    return (conf == null ? DEFAULT_USER_LOG_RETAIN_HOURS : conf.getInt(
        JobContext.USER_LOG_RETAIN_HOURS, DEFAULT_USER_LOG_RETAIN_HOURS));
  }

  /**
   * If the configuration is null or max user-log retain hours is not configured,
   * the retain hours are {@value UserLogCleaner#DEFAULT_USER_LOG_RETAIN_HOURS_MAX}
   */
  static int getUserlogRetainHoursMax(Configuration conf) {
    return (conf == null ? DEFAULT_USER_LOG_RETAIN_HOURS_MAX : conf.getInt(
        JobContext.USER_LOG_RETAIN_HOURS_MAX, DEFAULT_USER_LOG_RETAIN_HOURS_MAX));
  }

  /**
   * Adds job user-log directory to cleanup thread to delete logs after user-log
   * retain hours.
   * 
   * @param jobCompletionTime
   *          job completion time in millis
   * @param retainHours
   *          the user-log retain hours for the job
   * @param jobid
   *          JobID for which user logs should be deleted
   */
  public void markJobLogsForDeletion(long jobCompletionTime, int retainHours,
      org.apache.hadoop.mapreduce.JobID jobid) {
    long retainTimeStamp = jobCompletionTime + (retainHours * 1000L * 60L * 60L);
    LOG.info("Adding " + jobid + " for user-log deletion with retainTimeStamp:"
        + retainTimeStamp);
    completedJobs.put(jobid, Long.valueOf(retainTimeStamp));
  }

  /**
   * Remove job from user log deletion.
   * 
   * @param jobid
   */
  public void unmarkJobFromLogDeletion(JobID jobid) {
    if (completedJobs.remove(jobid) != null) {
      LOG.info("Removing " + jobid + " from user-log deletion");
    }
  }

  /**
   * Deletes the log path.
   * 
   * This path will be removed through {@link CleanupQueue}
   * 
   * @param logPath
   * @throws IOException
   */
  private void deleteLogPath(String logPath) throws IOException {
    IOException lioe = null;
    String logRoot = TaskLog.getUserLogDir().toString();
    final Path localLogPath = new Path(logRoot, logPath);
    if (LOG.isInfoEnabled()) {
      LOG.info("Deleting user log path " + localLogPath);
    }
    try {
     String user = localFs.getFileStatus(localLogPath).getOwner();
     TaskController controller = userLogManager.getTaskController();
     PathDeletionContext item = 
       new TaskController.DeletionContext(controller, true, user, logPath);
     cleanupQueue.addToQueue(item);
    } catch (IOException ioe) {
      lioe = ioe;
    }
    try {
      CentralTaskLogUtil.scheduleCleanup(cleanupQueue, localLogPath.toString());
    } catch (IOException ioe) {
      if (lioe == null) {
        throw ioe;
      } else {
        throw lioe;
      }
    }
  }
}
