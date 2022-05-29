package org.apache.hadoop.yarn.logaggregation.filecontroller.nodelocal;

import com.google.inject.Inject;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationHtmlBlock;
import org.apache.hadoop.yarn.util.Times;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class NodeLocalAggregatedLogsBlock extends LogAggregationHtmlBlock {

  private final Configuration conf;
  private final LogAggregationNodeLocalTFileController fileController;

  @Inject
  public NodeLocalAggregatedLogsBlock(ViewContext ctx, Configuration conf,
                                      LogAggregationNodeLocalTFileController fileController) {
    super(ctx);
    this.conf = conf;
    this.fileController = fileController;
  }

  @Override
  protected void render(Block html) {

    BlockParameters params = verifyAndParseParameters(html);
    if (params == null) {
      return;
    }

    List<FileStatus> nodeFiles;
    try {
      nodeFiles = fileController.getNodeLocalMetadataReader()
        .getLogsDirsFileStatusListForApp(params.getAppId(), params.getAppOwner());
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception ex) {
      html.h1("No logs available for container "
        + params.getContainerId().toString());
      return;
    }

    NodeId nodeId = params.getNodeId();

    String logEntity = params.getLogEntity();
    ApplicationId appId = params.getAppId();
    ContainerId containerId = params.getContainerId();
    long start = params.getStartIndex();
    long end = params.getEndIndex();
    long startTime = params.getStartTime();
    long endTime = params.getEndTime();

    boolean foundLog = false;
    String desiredLogType = $(CONTAINER_LOG_TYPE);
    try {
      for(FileStatus thisNodeFile : nodeFiles) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          if (nodeId != null && !nodeId.getHost().isEmpty()) {
            String nodeName = nodeId.getHost();
            if (nodeName.contains(":")) {
              nodeName = nodeName.substring(0, nodeName.indexOf(":"));
            }
            if (!thisNodeFile.getPath().toString()
              .contains(nodeName)) {
              continue;
            }
          }
          if (thisNodeFile.getPath().getName()
            .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
            continue;
          }
          long logUploadedTime = thisNodeFile.getModificationTime();
          if (logUploadedTime < startTime || logUploadedTime > endTime) {
            continue;
          }
          reader = new AggregatedLogFormat.LogReader(
            conf, thisNodeFile.getPath());

          String owner = null;
          Map<ApplicationAccessType, String> appAcls = null;
          try {
            owner = reader.getApplicationOwner();
            appAcls = reader.getApplicationAcls();
          } catch (IOException e) {
            LOG.error("Error getting logs for " + logEntity, e);
            continue;
          }
          String remoteUser = request().getRemoteUser();

          if (!checkAcls(conf, appId, owner, appAcls, remoteUser)) {
            html.h1().__("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity
              + " in log file [" + thisNodeFile.getPath().getName() + "]")
              .__();
            LOG.error("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity);
            continue;
          }

          AggregatedLogFormat.ContainerLogsReader logReader = reader
            .getContainerLogsReader(containerId);
          if (logReader == null) {
            continue;
          }

          foundLog = readContainerLogs(html, logReader, start, end,
            desiredLogType, logUploadedTime, startTime, endTime);
        } catch (IOException ex) {
          LOG.error("Error getting logs for " + logEntity, ex);
          continue;
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
      if (!foundLog) {
        if (desiredLogType.isEmpty()) {
          html.h1("No logs available for container "
            + containerId.toString());
        } else {
          html.h1("Unable to locate '" + desiredLogType
            + "' log for container " + containerId.toString());
        }
      }
    } catch (Exception e) {
      html.h1().__("Error getting logs for " + logEntity).__();
      LOG.error("Error getting logs for " + logEntity, e);
    }
  }

  private boolean readContainerLogs(Block html,
                                    AggregatedLogFormat.ContainerLogsReader logReader, long startIndex,
                                    long endIndex, String desiredLogType, long logUpLoadTime,
                                    long startTime, long endTime) throws IOException {
    int bufferSize = 65536;
    byte[] cbuf = new byte[bufferSize];

    boolean foundLog = false;
    String logType = logReader.nextLog();
    while (logType != null) {
      if (desiredLogType == null || desiredLogType.isEmpty()
        || desiredLogType.equals(logType)) {
        long logLength = logReader.getCurrentLogLength();
        if (foundLog) {
          html.pre().__("\n\n").__();
        }

        html.p().__("Log Type: " + logType).__();
        html.p().__("Log Upload Time: " + Times.format(logUpLoadTime)).__();
        html.p().__("Log Length: " + Long.toString(logLength)).__();

        long[] range = checkParseRange(html, startIndex, endIndex, startTime,
          endTime, logLength, logType);

        processContainerLog(html, range, logReader, bufferSize, cbuf);
        foundLog = true;
      }

      logType = logReader.nextLog();
    }

    return foundLog;
  }
}
