package org.apache.hadoop.yarn.logaggregation.filecontroller.nodelocal;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationDFSException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NODE_LOCAL_LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NODE_LOCAL_LOG_AGGREGATION_NODE_ID;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NODE_LOCAL_LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT;

public class LogAggregationNodeLocalTFileController extends LogAggregationFileController {
  private static final Logger LOG = LoggerFactory.getLogger(
    LogAggregationNodeLocalTFileController.class);

  private AggregatedLogFormat.LogWriter writer;
  private LogAggregationNodeLocalTFileController.TFileLogReader tfReader = null;

  private NodeLocalMetadataWriter nodeLocalMetadataWriter;
  private NodeLocalMetadataReader nodeLocalMetadataReader;

  private Path nodeLocalMetadataDir;
  private String nodeLocalMetadataDirPrefix;

  public LogAggregationNodeLocalTFileController(){}

  @Override
  protected void extractRemoteRootLogDir() {
    String remoteDir = conf.get(NODE_LOCAL_LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT, DEFAULT_NODE_LOCAL_LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT);
    String nodeId = conf.get(NODE_LOCAL_LOG_AGGREGATION_NODE_ID);
    if (nodeId == null || nodeId.isEmpty()) {
      // not init for readers
      return;
    }
    if(nodeId.contains(":")){
      nodeId = nodeId.substring(0, nodeId.indexOf(":"));
    }
    remoteRootLogDir = new Path(String.format(remoteDir, nodeId));
  }

  @Override
  protected void extractRemoteRootLogDirSuffix() {
    remoteRootLogDirSuffix = conf.get(
      YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
  }

  @Override
  public void initInternal(Configuration conf) {
    nodeLocalMetadataWriter = new NodeLocalMetadataWriter(conf);
    nodeLocalMetadataReader = new NodeLocalMetadataReader(conf);
    nodeLocalMetadataDir = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    nodeLocalMetadataDirPrefix = conf.get(YarnConfiguration.NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME,
      YarnConfiguration.DEFAULT_NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME);
  }

  @Override
  public void createAppDir(final String user, final ApplicationId appId,
                           UserGroupInformation userUgi) {
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          try {
            // TODO: Reuse FS for user?
            FileSystem remoteFS = getFileSystem(conf);

            Path appDir = getRemoteNodeLogFileForAppForNodeLocalAggregator(appId, user);
            appDir = appDir.makeQualified(remoteFS.getUri(), remoteFS.getWorkingDirectory());
            if (!checkExists(remoteFS, appDir, APP_DIR_PERMISSIONS)) {
              createDir(remoteFS, appDir, APP_DIR_PERMISSIONS);
            }
          } catch (IOException e) {
            LOG.error("Failed to setup application log directory for "
              + appId, e);
            throw e;
          }
          return null;
        }
      });
    } catch (Exception e) {
      if (e instanceof RemoteException) {
        throw new YarnRuntimeException(((RemoteException) e)
          .unwrapRemoteException(SecretManager.InvalidToken.class));
      }
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void initializeWriter(LogAggregationFileControllerContext context) throws IOException {
    this.writer = new AggregatedLogFormat.LogWriter();
    writer.initialize(this.conf, context.getRemoteNodeTmpLogFileForApp(),
      context.getUserUgi());
    // Write ACLs once when the writer is created.
    writer.writeApplicationACLs(context.getAppAcls());
    writer.writeApplicationOwner(context.getUserUgi().getShortUserName());
  }

  @Override
  public void closeWriter() throws LogAggregationDFSException {
    if (this.writer != null) {
      try {
        this.writer.close();
      } catch (DSQuotaExceededException e) {
        throw new LogAggregationDFSException(e);
      } finally {
        this.writer = null;
      }
    }
  }

  @Override
  public void write(AggregatedLogFormat.LogKey logKey, AggregatedLogFormat.LogValue logValue) throws IOException {
    this.writer.append(logKey, logValue);
  }

  @Override
  public void postWrite(final LogAggregationFileControllerContext record)
    throws Exception {
    // Write metadata about completed containers
    try {
      nodeLocalMetadataWriter.write(record.getAppId(), record.getNodeId(), new ArrayList<>(record.getUploadedContainersList()), record.getUserUgi().getUserName());
    } catch (IOException e) {
      LOG.error("Failed to write metadata info for " + record.getAppId(), e);
    }

    // Before upload logs, make sure the number of existing logs
    // is smaller than the configured NM log aggregation retention size.
    if (record.isUploadedLogsInThisCycle() &&
      record.isLogAggregationInRolling()) {
      cleanOldLogs(record.getRemoteNodeLogFileForApp(), record.getNodeId(),
        record.getUserUgi());
      record.increcleanupOldLogTimes();
    }

    // close the writer before the file is renamed or deleted
    closeWriter();

    final Path renamedPath = record.getRollingMonitorInterval() <= 0
      ? record.getRemoteNodeLogFileForApp() : new Path(
      record.getRemoteNodeLogFileForApp().getParent(),
      record.getRemoteNodeLogFileForApp().getName() + "_"
        + record.getLogUploadTimeStamp());
    final boolean rename = record.isUploadedLogsInThisCycle();
    try {
      record.getUserUgi().doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FileSystem remoteFS = record.getRemoteNodeLogFileForApp()
            .getFileSystem(conf);
          if (rename) {
            remoteFS.rename(record.getRemoteNodeTmpLogFileForApp(),
              renamedPath);
          } else {
            remoteFS.delete(record.getRemoteNodeTmpLogFileForApp(), false);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error(
        "Failed to move temporary log file to final location: ["
          + record.getRemoteNodeTmpLogFileForApp() + "] to ["
          + renamedPath + "]", e);
      throw new Exception("Log uploaded failed for Application: "
        + record.getAppId() + " in NodeManager: "
        + LogAggregationUtils.getNodeString(record.getNodeId()) + " at "
        + Times.format(record.getLogUploadTimeStamp()) + "\n");
    }
  }

  @Override
  public boolean readAggregatedLogs(ContainerLogsRequest logRequest, OutputStream os) throws IOException {
    boolean findLogs = false;
    boolean createPrintStream = (os == null);
    ApplicationId appId = logRequest.getAppId();
    String nodeId = logRequest.getNodeId();
    List<String> logTypes = new ArrayList<>();
    if (logRequest.getLogTypes() != null && !logRequest
      .getLogTypes().isEmpty()) {
      logTypes.addAll(logRequest.getLogTypes());
    }
    String containerIdStr = logRequest.getContainerId();
    boolean getAllContainers = (containerIdStr == null
      || containerIdStr.isEmpty());
    long size = logRequest.getBytes();
    List<FileStatus> nodeFiles = nodeLocalMetadataReader.getLogsDirsFileStatusListForApp(appId, logRequest.getAppOwner());
    byte[] buf = new byte[65535];
    for (FileStatus thisNodeFile : nodeFiles) {
      String nodeName = nodeId;
      if (nodeName != null && nodeName.contains(":")) {
        nodeName = nodeName.substring(0, nodeId.indexOf(":"));
      }
      String logName = thisNodeFile.getPath().getName();
      if ((nodeId == null || nodeId.isEmpty() || thisNodeFile.getPath().toString().contains(nodeName)) && !logName.endsWith(
        LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          reader = new AggregatedLogFormat.LogReader(conf,
            thisNodeFile.getPath());
          DataInputStream valueStream;
          AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
          valueStream = reader.next(key);
          while (valueStream != null) {
            if (getAllContainers || (key.toString().equals(containerIdStr))) {
              if (createPrintStream) {
                os = LogToolUtils.createPrintStream(
                  logRequest.getOutputLocalDir(),
                  nodeName, key.toString());
              }
              try {
                while (true) {
                  try {
                    String fileType = valueStream.readUTF();
                    String fileLengthStr = valueStream.readUTF();
                    long fileLength = Long.parseLong(fileLengthStr);
                    if (logTypes == null || logTypes.isEmpty() ||
                      logTypes.contains(fileType)) {
                      LogToolUtils.outputContainerLog(key.toString(),
                        nodeName, fileType, fileLength, size,
                        Times.format(thisNodeFile.getModificationTime()),
                        valueStream, os, buf,
                        ContainerLogAggregationType.AGGREGATED);
                      byte[] b = aggregatedLogSuffix(fileType).getBytes(
                        Charset.forName("UTF-8"));
                      os.write(b, 0, b.length);
                      findLogs = true;
                    } else {
                      long totalSkipped = 0;
                      long currSkipped = 0;
                      while (currSkipped != -1 && totalSkipped < fileLength) {
                        currSkipped = valueStream.skip(
                          fileLength - totalSkipped);
                        totalSkipped += currSkipped;
                      }
                    }
                  } catch (EOFException eof) {
                    break;
                  }
                }
              } finally {
                os.flush();
                if (createPrintStream) {
                  closePrintStream(os);
                }
              }
              if (!getAllContainers) {
                break;
              }
            }
            // Next container
            key = new AggregatedLogFormat.LogKey();
            valueStream = reader.next(key);
          }
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    }
    return findLogs;
  }

  @Override
  public List<ContainerLogMeta> readAggregatedLogsMeta(ContainerLogsRequest logRequest) throws IOException {
    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    String containerIdStr = logRequest.getContainerId();
    String nodeId = logRequest.getNodeId();
    String nodeName = nodeId;
    if (nodeName != null && nodeName.contains(":")) {
      nodeName = nodeName.substring(0, nodeId.indexOf(":"));
    }
    ApplicationId appId = logRequest.getAppId();
    String appOwner = logRequest.getAppOwner();
    ApplicationAttemptId appAttemptId = logRequest.getAppAttemptId();
    boolean getAllContainers = (containerIdStr == null &&
      appAttemptId == null);
    boolean getOnlyOneContainer = containerIdStr != null;
    String nodeIdStr = (nodeId == null || nodeId.isEmpty()) ? null
      : nodeName;
    List<FileStatus> nodeFiles = nodeLocalMetadataReader.getLogsDirsFileStatusListForApp(appId, appOwner);
    if (nodeFiles == null) {
      throw new IOException("There is no available log file for "
        + "application:" + appId);
    }
    for(FileStatus thisNodeFile : nodeFiles) {
      String thisNodeName = nodeLocalMetadataReader.getNodeFromFilePath(thisNodeFile);
      if (nodeIdStr != null) {
        if (!thisNodeFile.getPath().toString().contains(nodeIdStr)) {
          continue;
        }
      }
      if (!thisNodeFile.getPath().getName()
        .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader =
          new AggregatedLogFormat.LogReader(conf,
            thisNodeFile.getPath());
        try {
          DataInputStream valueStream;
          AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
          valueStream = reader.next(key);
          while (valueStream != null) {
            if (getAllContainers || (key.toString().equals(containerIdStr)) ||
              belongsToAppAttempt(appAttemptId, key.toString())) {
              ContainerLogMeta containerLogMeta = new ContainerLogMeta(
                key.toString(), thisNodeName);
              while (true) {
                try {
                  Pair<String, String> logMeta =
                    AggregatedLogFormat.LogReader.readContainerMetaDataAndSkipData(
                      valueStream);
                  containerLogMeta.addLogMeta(
                    logMeta.getFirst(),
                    logMeta.getSecond(),
                    Times.format(thisNodeFile.getModificationTime()));
                } catch (EOFException eof) {
                  break;
                }
              }
              containersLogMeta.add(containerLogMeta);
              if (getOnlyOneContainer) {
                break;
              }
            }
            // Next container
            key = new AggregatedLogFormat.LogKey();
            valueStream = reader.next(key);
          }
        } finally {
          reader.close();
        }
      }
    }
    return containersLogMeta;
  }

  @Override
  public void renderAggregatedLogsBlock(HtmlBlock.Block html, View.ViewContext context) {
    NodeLocalAggregatedLogsBlock block = new NodeLocalAggregatedLogsBlock(
      context, conf, this);
    block.render(html);
  }

  @Override
  public Path getRemoteNodeLogFileForApp(ApplicationId appId, String user,
                                         NodeId nodeId) {
    return new Path(getRemoteNodeLogFileForAppForNodeLocalAggregator(appId, user), remoteRootLogDirSuffix);
  }

  Path getRemoteNodeLogFileForAppForNodeLocalAggregator(ApplicationId appId, String user) {
    return new Path(this.remoteRootLogDir + "/" + user + "/" + appId.toString());
  }

  @Override
  public Path getRemoteAppLogDir(ApplicationId appId, String appOwner)
    throws IOException {
    Path remoteAppDir = null;
    if (appOwner == null) {
      Path qualifiedRemoteRootLogDir =
        FileContext.getFileContext(conf).makeQualified(remoteRootLogDir);
      FileContext fc = FileContext.getFileContext(
        qualifiedRemoteRootLogDir.toUri(), conf);
      Path toMatch = nodeLocalMetadataReader.getPathForApp(appId.toString(), "*");
      FileStatus[] matching  = fc.util().globStatus(toMatch);
      if (matching == null || matching.length != 1) {
        throw new IOException("Can not find remote application directory for "
          + "the application:" + appId);
      }
      remoteAppDir = matching[0].getPath();
    } else {
      remoteAppDir = nodeLocalMetadataReader.getPathForApp(appId.toString(), appOwner);
    }
    return remoteAppDir;
  }

  @Override
  public Path getOlderRemoteAppLogDir(ApplicationId appId, String appOwner)
    throws IOException {
    Path remoteRootLogDirOlder = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String remoteRootLogDirSuffixOlder = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);

    return LogAggregationUtils.getOlderRemoteAppLogDir(conf, appId, appOwner,
      remoteRootLogDirOlder, remoteRootLogDirSuffixOlder);
  }

  @Override
  protected void cleanOldLogs(Path remoteNodeLogFileForApp,
                              final NodeId nodeId, UserGroupInformation userUgi) {
    try {
      final FileSystem remoteFS = remoteNodeLogFileForApp.getFileSystem(conf);
      Path appDir = remoteNodeLogFileForApp.getParent().makeQualified(
        remoteFS.getUri(), remoteFS.getWorkingDirectory());
      Set<FileStatus> status =
        new HashSet<FileStatus>(Arrays.asList(remoteFS.listStatus(appDir)));

      status = status.stream().filter(
        next -> next.getPath().getName()
          .contains(this.remoteRootLogDirSuffix)
          && !next.getPath().getName().endsWith(
          LogAggregationUtils.TMP_FILE_SUFFIX)).collect(
        Collectors.toSet());
      // Normally, we just need to delete one oldest log
      // before we upload a new log.
      // If we can not delete the older logs in this cycle,
      // we will delete them in next cycle.
      if (status.size() >= this.retentionSize) {
        // sort by the lastModificationTime ascending
        List<FileStatus> statusList = new ArrayList<FileStatus>(status);
        Collections.sort(statusList, new Comparator<FileStatus>() {
          public int compare(FileStatus s1, FileStatus s2) {
            return s1.getModificationTime() < s2.getModificationTime() ? -1
              : s1.getModificationTime() > s2.getModificationTime() ? 1 : 0;
          }
        });
        for (int i = 0; i <= statusList.size() - this.retentionSize; i++) {
          final FileStatus remove = statusList.get(i);
          try {
            userUgi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                remoteFS.delete(remove.getPath(), false);
                return null;
              }
            });
          } catch (Exception e) {
            LOG.error("Failed to delete " + remove.getPath(), e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to clean old logs", e);
    }
  }

  public NodeLocalMetadataReader getNodeLocalMetadataReader() {
    return nodeLocalMetadataReader;
  }

  public Path getNodeLocalMetadataDir() {
    return nodeLocalMetadataDir;
  }

  public String getNodeLocalMetadataDirPrefix() {
    return nodeLocalMetadataDirPrefix;
  }

  @Override
  public String getApplicationOwner(Path aggregatedLog, ApplicationId appId)
    throws IOException {
    createTFileLogReader(aggregatedLog);
    return this.tfReader.getLogReader().getApplicationOwner();
  }

  @Override
  public Map<ApplicationAccessType, String> getApplicationAcls(
    Path aggregatedLog, ApplicationId appId) throws IOException {
    createTFileLogReader(aggregatedLog);
    return this.tfReader.getLogReader().getApplicationAcls();
  }

  private void createTFileLogReader(Path aggregatedLog) throws IOException {
    if (this.tfReader == null || !this.tfReader.getAggregatedLogPath()
      .equals(aggregatedLog)) {
      AggregatedLogFormat.LogReader logReader = new AggregatedLogFormat.LogReader(conf, aggregatedLog);
      this.tfReader = new LogAggregationNodeLocalTFileController.TFileLogReader(logReader, aggregatedLog);
    }
  }

  private static class TFileLogReader {
    private AggregatedLogFormat.LogReader logReader;
    private Path aggregatedLogPath;

    TFileLogReader(AggregatedLogFormat.LogReader logReader, Path aggregatedLogPath) {
      this.setLogReader(logReader);
      this.setAggregatedLogPath(aggregatedLogPath);
    }
    public AggregatedLogFormat.LogReader getLogReader() {
      return logReader;
    }
    public void setLogReader(AggregatedLogFormat.LogReader logReader) {
      this.logReader = logReader;
    }
    public Path getAggregatedLogPath() {
      return aggregatedLogPath;
    }
    public void setAggregatedLogPath(Path aggregatedLogPath) {
      this.aggregatedLogPath = aggregatedLogPath;
    }
  }
}
