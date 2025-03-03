/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.maprfs.AbstractMapRFileSystem;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.BaseMapRUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.api.records.ApplicationId.appIdStrPrefix;

/**
 * Implementation of the node local volume monitor. It periodically tracks the
 * MFS local volume of the node and cleans up it.
 */
public class NodeLocalVolumeMonitorImpl extends AbstractService implements
    NodeLocalVolumeMonitor {

  /**
   * Logging infrastructure.
   */
  final static Logger LOG =
      LoggerFactory.getLogger(NodeLocalVolumeMonitorImpl.class);

  /**
   * Interval to monitor the node local volume.
   */
  private long monitoringInterval;
  private long monitorExpiration;
  private long startSleepTime;
  /**
   * Thread to monitor the node local volume.
   */
  private MonitoringThread monitoringThread;

  private Context nmContext;
  private FileSystem fs;
  private Set<ApplicationId> applicationSet;
  private Set<String> jobSet;
  private String outputDirName;
  private String outputUDirName;
  private String spillDirName;
  private String spillUDirName;
  private String mapredLocalVolumeMountPath;
  private String mapredNMVolumeMountPath;

  private static final String LOCALHOSTNAME = BaseMapRUtil.getMapRHostName();

  // The below config params should be used by consumers of the meta data as well.
  public static final String MAPR_LOCALOUTPUT_DIR_PARAM = "mapr.localoutput.dir";
  public static final String MAPR_LOCALOUTPUT_DIR_DEFAULT = "output";

  public static final String MAPR_LOCALSPILL_DIR_PARAM = "mapr.localspill.dir";
  public static final String MAPR_LOCALSPILL_DIR_DEFAULT = "spill";

  public static final String MAPR_UNCOMPRESSED_SUFFIX = ".U";

  public static final String MAPR_MAPRED_LOCAL_VOLUME_PATH = "mapr.mapred.localvolume.mount.path";
  public static final String MAPR_NM_LOCAL_VOLUME_PATH = "mapr.mapred.localvolume.root.dir.path";

  public static final String jobIdStrPrefix = "job";

  /**
   * Initialize the node local volume monitor.
   */
  public NodeLocalVolumeMonitorImpl(Context context) {
    super(NodeLocalVolumeMonitorImpl.class.getName());
    this.nmContext = context;
    this.monitoringThread = new MonitoringThread();
  }

  /**
   * Initialize the service with the proper parameters.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.mapredLocalVolumeMountPath =
        conf.get(MAPR_MAPRED_LOCAL_VOLUME_PATH, "/var/mapr/local/" + LOCALHOSTNAME + "/mapred");
    this.mapredNMVolumeMountPath =
        conf.get(MAPR_NM_LOCAL_VOLUME_PATH, mapredLocalVolumeMountPath + "/nodeManager");

    this.outputDirName = mapredNMVolumeMountPath + "/" +
        conf.get(MAPR_LOCALOUTPUT_DIR_PARAM, MAPR_LOCALOUTPUT_DIR_DEFAULT);
    this.outputUDirName = this.outputDirName + MAPR_UNCOMPRESSED_SUFFIX;
    this.spillDirName = mapredNMVolumeMountPath + "/" +
        conf.get(MAPR_LOCALSPILL_DIR_PARAM, MAPR_LOCALSPILL_DIR_DEFAULT);
    this.spillUDirName = this.spillDirName + MAPR_UNCOMPRESSED_SUFFIX;

    if (fs == null) {
      this.fs = FileSystem.get(new URI(outputDirName), conf);
    }
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_LOCAL_VOLUME_MON_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_NM_LOCAL_VOLUME_MON_INTERVAL_SECS);
    this.monitorExpiration =
        conf.getLong(YarnConfiguration.NM_LOCAL_VOLUME_MON_EXPIRE_SECS,
            YarnConfiguration.DEFAULT_NM_LOCAL_VOLUME_MON_EXPIRE_SECS);

    this.startSleepTime =
        conf.getLong(YarnConfiguration.NM_LOCAL_VOLUME_MON_START_SLEEP_SECS,
            YarnConfiguration.DEFAULT_NM_LOCAL_VOLUME_MON_START_SLEEP_SECS);

    LOG.info("Local volume monitor initialized. Monitor interval: {} seconds", monitoringInterval);
  }

  /**
   * Check if we should be monitoring.
   *
   * @return <em>true</em> if we can monitor the node local volume.
   */
  private boolean isEnabled() {
    if (this.monitoringInterval <= 0) {
      LOG.info("Node local volume monitoring interval is <=0. {} is disabled.", this.getClass().getName());
      return false;
    }
    if (!(fs instanceof AbstractMapRFileSystem)) {
      LOG.info("Output directory for local volume is not MFS instance. Monitor service can't initialize.");
      return false;
    }
    return true;
  }

  /**
   * Start the thread that does the node local volume monitoring.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * Stop the thread that does the node local volume monitoring.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * Thread that monitors the local volume of this node.
   */
  private class MonitoringThread extends Thread {
    /**
     * Initialize the node local volume monitoring thread.
     */
    public MonitoringThread() {
      super("Node Local Volume Monitor");
      this.setDaemon(true);
    }

    /**
     * Periodically monitor the local volume of the node and cleans data for finished apps.
     */
    @Override
    public void run() {
      // Sleep requires for loading state store and works with final state
      try {
        Thread.sleep(startSleepTime * 1000);
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted during start. Exiting.", NodeLocalVolumeMonitorImpl.class.getName());
      }
      while (true) {
        LOG.info("Cleanup node local volume is starting");
        applicationSet = nmContext.getApplications().keySet();
        jobSet = new HashSet<>();
        for (ApplicationId appId : applicationSet) {
          if (nmContext.getApplications().get(appId).getApplicationState() != ApplicationState.FINISHED) {
            jobSet.add(appId.toString().replaceAll(appIdStrPrefix, jobIdStrPrefix));
          }
        }
        checkLocalVolumeDir();
        LOG.info("Cleanup node local volume end");
        try {
          Thread.sleep(monitoringInterval * 1000);
        } catch (InterruptedException e) {
          LOG.warn("{} is interrupted. Exiting.", NodeLocalVolumeMonitorImpl.class.getName());
          break;
        }
      }
    }
  }
/**
 * Get file status for local volume directories and clean old data
 * */
  private void checkLocalVolumeDir() {
    try {
      cleanUpDirectory(fs.listStatus(new Path(spillDirName)));
      cleanUpDirectory(fs.listStatus(new Path(spillUDirName)));
      cleanUpDirectory(fs.listStatus(new Path(outputDirName)));
      cleanUpDirectory(fs.listStatus(new Path(outputUDirName)));
    } catch (IOException e) {
      LOG.warn("Unable to locate local volume directories.");
    }
  }

  /**
   * Clean directories that are older than the expiry date
   * */
  private void cleanUpDirectory(FileStatus[] dirs) {
    long borderToDelete = new Date().getTime() - TimeUnit.SECONDS.toMillis(monitorExpiration);
    if (dirs != null && dirs.length > 0) {
      for (FileStatus dir : dirs) {
        if (dir.getModificationTime() < borderToDelete && !jobSet.contains(dir.getPath().getName())) {
          try {
            if (fs.delete(dir.getPath(), true)) {
              LOG.debug("Successfully deleted : {}", dir.getPath());
            } else {
              LOG.warn("Directory {} was not remove during cleaning by monitor tool.", dir.getPath());
            }
          } catch (IOException e) {
            LOG.warn("Unable to delete files: {}", dir.getPath());
          }
        }
      }
    }
  }
}
