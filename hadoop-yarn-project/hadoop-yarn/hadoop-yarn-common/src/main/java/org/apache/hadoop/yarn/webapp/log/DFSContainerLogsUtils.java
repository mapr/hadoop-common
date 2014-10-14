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

package org.apache.hadoop.yarn.webapp.log;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.TaskLogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utilities for fetching a user's log file from DFS.
 */
public class DFSContainerLogsUtils {
  public static final Logger LOG = LoggerFactory.getLogger(DFSContainerLogsUtils.class);
  public static List<Path> getContainerLogDirs(ContainerId containerId)
    throws IOException {

    return Arrays.asList(TaskLogUtil.getDFSLoggingHandler()
        .getLogDir(containerId));
  }

  /**
   * Finds the log file with the given filename for the given container.
   */
  public static Path getContainerLogFile(ContainerId containerId,
      String fileName, String remoteUser) throws IOException {

    return new Path(TaskLogUtil.getDFSLoggingHandler()
        .getLogDir(containerId), fileName);
  }

  public static InputStream openLogFileForRead(String containerIdStr,
      final Path logFile, final String user)
    throws IOException {

    UserGroupInformation ugi = null;
    // Impersonate as given user only if security is enabled. This is because
    // in unsecure mode, the given user is "Unknown".
    if (UserGroupInformation.isSecurityEnabled()) {
      ugi = UserGroupInformation.createProxyUser(user,
          UserGroupInformation.getLoginUser());
    } else {
      ugi = UserGroupInformation.getLoginUser();
    }

    try {
      return ugi.doAs(new PrivilegedExceptionAction<InputStream>() {
        public InputStream run() throws Exception {
          FileSystem fs = FileSystem.get(TaskLogUtil.getConf());
          return fs.open(logFile);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  public static long getFileLength(Path logFile) throws IOException {
    FileSystem fs = FileSystem.get(TaskLogUtil.getConf());
    FileStatus fileStatus = fs.listStatus(logFile)[0];
    return fileStatus.getLen();
  }

  public static Path[] getFilesInDir(Path dir) throws IOException {
    Path[] paths = null;

    FileSystem fs = FileSystem.get(TaskLogUtil.getConf());
    FileStatus[] fileStatusArr = fs.listStatus(dir);
    if (fileStatusArr != null) {
      paths = new Path[fileStatusArr.length];
      for (int i = 0; i < fileStatusArr.length; i++) {
        paths[i] = fileStatusArr[i].getPath();
      }
    }

    return paths;
  }
}
