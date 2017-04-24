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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.PermissionNotMatchException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.TaskLogUtil;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.log.DFSContainerLogsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utilities for fetching a user's log file in a secure fashion.
 * It supports retrieving logs from both local file system and distributed file
 * system depending on where the logs are stored.
 */
public class ContainerLogsUtils {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerLogsUtils.class);
  
  /**
   * Finds the directories that logs for the given container are stored
   * on.
   */
  public static List<Path> getContainerLogDirs(ContainerId containerId,
      String remoteUser, Context context) throws YarnException {
    Container container = context.getContainers().get(containerId);

    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    // It is not required to have null check for container ( container == null )
    // and throw back exception.Because when container is completed, NodeManager
    // remove container information from its NMContext.Configuring log
    // aggregation to false, container log view request is forwarded to NM. NM
    // does not have completed container information,but still NM serve request for
    // reading container logs. 
    if (container != null) {
      checkState(container.getContainerState());

      if (isDfsLoggingEnabled(containerId, context)) {
        try {
          return DFSContainerLogsUtils.getContainerLogDirs(containerId);
        } catch (IOException e) {
          LOG.warn("Failed to find logs for container: " + containerId, e);
          throw new NotFoundException("Cannot find logs for container: "
              + containerId);
        }
      }
    }

    return getContainerLogDirs(containerId, context.getLocalDirsHandler());
  }

  static List<Path> getContainerLogDirs(ContainerId containerId,
      LocalDirsHandlerService dirsHandler) throws YarnException {
    List<String> logDirs = dirsHandler.getLogDirsForRead();
    List<Path> containerLogDirs = new ArrayList<Path>(logDirs.size());
    for (String logDir : logDirs) {
      logDir = new File(logDir).toURI().getPath();
      String appIdStr = ConverterUtils.toString(containerId
          .getApplicationAttemptId().getApplicationId());
      Path appLogDir = new Path(logDir, appIdStr);
      containerLogDirs.add(new Path(appLogDir, containerId.toString()));
    }
    return containerLogDirs;
  }
  
  /**
   * Finds the log file with the given filename for the given container.
   */
  public static Path getContainerLogFile(ContainerId containerId,
      String fileName, String remoteUser, Context context) throws YarnException {
    
    if (isDfsLoggingEnabled(containerId, context)) {
      try {
        return DFSContainerLogsUtils.getContainerLogFile(containerId, fileName, remoteUser);
      } catch (IOException e) {
        LOG.warn("Failed to find logs for container: "
            + containerId + " file: " + fileName, e);
        throw new NotFoundException("Cannot find logs for container: "
            + containerId + " file: " + fileName);
      }
    }

    Container container = context.getContainers().get(containerId);
    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    if (container != null) {
      checkState(container.getContainerState());
    }
    
    String relativeContainerLogDir = ContainerLaunch.getRelativeContainerLogDir(
        application.getAppId().toString(), containerId.toString());

    String relativeFileName = relativeContainerLogDir + Path.SEPARATOR + fileName;

    try {
      LocalDirsHandlerService dirsHandler = context.getLocalDirsHandler();
      return dirsHandler.getLogPathToRead(relativeFileName);
    } catch (IOException e) {
      LOG.warn("Failed to find log file", e);
      throw new NotFoundException("Cannot find this log on the local disk.");
    }
  }
  
  private static Application getApplicationForContainer(ContainerId containerId,
      Context context) {
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    Application application = context.getApplications().get(
        applicationId);
    
    if (application == null) {
      throw new NotFoundException(
          "Unknown container. Container either has not started or "
              + "has already completed or "
              + "doesn't belong to this node at all.");
    }
    return application;
  }
  
  private static void checkAccess(String remoteUser, Application application,
      Context context) throws YarnException {
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !context.getApplicationACLsManager().checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, application.getUser(),
            application.getAppId())) {
      throw new YarnException(
          "User [" + remoteUser
              + "] is not authorized to view the logs for application "
              + application.getAppId());
    }
  }
  
  private static void checkState(ContainerState state) {
    if (state == ContainerState.NEW || state == ContainerState.LOCALIZING ||
        state == ContainerState.LOCALIZED) {
      throw new NotFoundException("Container is not yet running. Current state is "
          + state);
    }
    if (state == ContainerState.LOCALIZATION_FAILED) {
      throw new NotFoundException("Container wasn't started. Localization failed.");
    }
  }
  
  public static InputStream openLogFileForRead(String containerIdStr, Path logFile,
      String remoteUser, Context context) throws IOException {
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    
    if (isDfsLoggingEnabled(containerId, context)) {
      return DFSContainerLogsUtils.openLogFileForRead(containerIdStr, logFile,
          remoteUser);
    }

    try {
      File file = new File(logFile.toString());
      String remoteUserPrimGroup = null;
      if (UserGroupInformation.isSecurityEnabled()) {
        remoteUserPrimGroup = UserGroupInformation.createRemoteUser(remoteUser)
                .getPrimaryGroupName();
      }
      return SecureIOUtils.openForRead(file, remoteUser, remoteUserPrimGroup);
    } catch (PermissionNotMatchException ex) {
      LOG.error(
            "Exception reading log file " + logFile, ex);
      throw new IOException("Exception reading log file. User '"
            + remoteUser
            + "' doesn't own requested log file : "
            + logFile.toString(), ex);
    } catch (IOException e) {
        throw new IOException("Exception reading log file. It might be because log "
            + "file was aggregated : " + logFile.toString(), e);
    }
  }

  public static long getFileLength(Path logFile, ContainerId containerId,
      Context context) throws IOException {

    if (isDfsLoggingEnabled(containerId, context)) {
      return DFSContainerLogsUtils.getFileLength(logFile);
    }

    return new File(logFile.toString()).length();
  }

  public static Path[] getFilesInDir(Path dir, ContainerId containerId,
      Context context) throws IOException {

    if (isDfsLoggingEnabled(containerId, context)) {
      return DFSContainerLogsUtils.getFilesInDir(dir);
    }

    Path[] paths = null;
    File[] files = new File(dir.toString()).listFiles();
    if (files != null) {
      paths = new Path[files.length];
      for (int i = 0; i < files.length; i++) {
        paths[i] = new Path(files[i].getAbsolutePath());
      }
    }

    return paths;
  }

  private static boolean isDfsLoggingEnabled(ContainerId containerId,
      Context context) {

    Container container = context.getContainers().get(containerId);
    if (container == null) {
      // This means the container is completed. So the logs should be
      // retrieved through history server.
      return false;
    }

    ContainerLaunchContext launchContext = container.getLaunchContext();
    Map<String, String> env = launchContext.getEnvironment();
    if (env != null) {
      if (TaskLogUtil.isDfsLoggingEnabled(env)) {
        return true;
      }
    }
    return false;
  }
}
