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
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.TaskLogUtil;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.PRE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * HTML block used to display logs retrieved from DFS.
 *
 * Most of the formatting code is taken from
 * {@link ContainerLogsPage.ContainersLogsBlock}.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class DFSContainerLogsBlock extends HtmlBlock
  implements YarnWebParams {

  @Override
  protected void render(Block html) {
    if (!TaskLogUtil.isDfsLoggingEnabled()) {
      html.h1()
        ._("Logs not found. DFS Logging is not enabled.")
        ._();
      return;
    }

    ContainerId containerId;
    try {
      containerId = ConverterUtils.toContainerId($(CONTAINER_ID));
    } catch (IllegalArgumentException ex) {
      html.h1("Invalid container ID: " + $(CONTAINER_ID));
      return;
    }

    try {
      if ($(CONTAINER_LOG_TYPE).isEmpty()) {
        List<Path> logFiles = DFSContainerLogsUtils.getContainerLogDirs(containerId);
        try {
          printLogFileDirectory(html, logFiles);
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      } else {
        String user = request().getRemoteUser();
        Path logFile = DFSContainerLogsUtils.getContainerLogFile(containerId,
            $(CONTAINER_LOG_TYPE), user);
        try {
          printLogFile(html, logFile, user);
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      }
    } catch (NotFoundException | YarnRuntimeException ex) {
      html.h1(ex.getMessage());
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private void printLogFile(Block html, Path logFile, String user)
    throws IOException {

    long length = DFSContainerLogsUtils.getFileLength(logFile);

    long start =
      $("start").isEmpty() ? -4 * 1024 : Long.parseLong($("start"));
    start = start < 0 ? length + start : start;
    start = start < 0 ? 0 : start;
    long end =
      $("end").isEmpty() ? length : Long.parseLong($("end"));
    end = end < 0 ? length + end : end;
    end = end < 0 ? length : end;
    if (start > end) {
      html.h1("Invalid start and end values. Start: [" + start + "]"
          + ", end[" + end + "]");
      return;
    } else {
      InputStream logByteStream = null;

      try {
        logByteStream = DFSContainerLogsUtils.openLogFileForRead($(CONTAINER_ID),
            logFile, user);
      } catch (IOException ex) {
        html.h1(ex.getMessage());
        return;
      }

      try {
        long toRead = end - start;
        if (toRead < length) {
          html.p()._("Showing " + toRead + " bytes. Click ")
            .a(url("logs", $(NM_NODENAME), $(CONTAINER_ID),
                  $(ENTITY_STRING), $(APP_OWNER),
                  logFile.getName(), "?start=0"), "here").
            _(" for full log")._();
        }

        IOUtils.skipFully(logByteStream, start);
        InputStreamReader reader = new InputStreamReader(logByteStream);
        int bufferSize = 65536;
        char[] cbuf = new char[bufferSize];

        int len = 0;
        int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
        PRE<Hamlet> pre = html.pre();

        while ((len = reader.read(cbuf, 0, currentToRead)) > 0
            && toRead > 0) {
          pre._(new String(cbuf, 0, len));
          toRead = toRead - len;
          currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
            }

        pre._();
        reader.close();

      } catch (IOException e) {
        LOG.error(
            "Exception reading log file " + logFile, e);
        html.h1("Exception reading log file" + logFile);
      } finally {
        if (logByteStream != null) {
          try {
            logByteStream.close();
          } catch (IOException e) {
            // Ignore
          }
        }
      }
    }
  }

  private void printLogFileDirectory(Block html, List<Path> containerLogsDirs)
    throws IOException {

    // Print out log types in lexical order
    Collections.sort(containerLogsDirs);
    boolean foundLogFile = false;
    for (Path containerLogsDir : containerLogsDirs) {
      Path[] logFiles = DFSContainerLogsUtils.getFilesInDir(containerLogsDir);
      if (logFiles != null) {
        Arrays.sort(logFiles);
        for (Path logFile : logFiles) {
          String logName = logFile.getName();
          long length = DFSContainerLogsUtils.getFileLength(logFile);
          foundLogFile = true;
          html.p()
            .a(url("logs", $(NM_NODENAME), $(CONTAINER_ID),
                  $(ENTITY_STRING), $(APP_OWNER),
                  logName, "?start=-4096"),
                logName + " : Total file length is "
                + length + " bytes.")._();
        }
      }
    }
    if (!foundLogFile) {
      html.h1("No logs available for container " + $(CONTAINER_ID));
      return;
    }
  }
}
