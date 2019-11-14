package org.apache.hadoop.yarn.logaggregation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeLocalMetadataReader {
  private static final Log LOG = LogFactory.getLog(NodeLocalMetadataReader.class);

  private final String prefixPath;
  private final String logMetaDir;
  private final String containersFile;

  private final Configuration conf;
  private FileSystem fs;

  public NodeLocalMetadataReader(Configuration conf) {
    this.conf = conf;
    this.prefixPath = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR);
    this.logMetaDir = conf.get(YarnConfiguration.NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME,
            YarnConfiguration.DEFAULT_NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME);
    this.containersFile = conf.get(YarnConfiguration.NODE_LOCAL_AGGREGATION_METADATA_FILENAME,
        YarnConfiguration.DEFAULT_NODE_LOCAL_AGGREGATION_METADATA_FILENAME);
    init(conf);
  }

  private void init(Configuration conf) {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Cannot create filesystem instance", e);
    }
  }

  public Map<String, List<String>> getLogMetadataForApplication(String applicationId, String appOwner) throws IOException {
    Path applicationDir = getPathForApp(applicationId, appOwner);
    if (!fs.exists(applicationDir)) {
      LOG.info("No metadata found for application: " + applicationId);
      return new HashMap<>();
    }

    Map<String, List<String>> nodeToContainersMap = new HashMap<>();

    FileStatus[] nodeLogsMetadata = fs.listStatus(applicationDir);

    for (FileStatus nodeLogs : nodeLogsMetadata) {
      Path metadataFile = new Path(nodeLogs.getPath(), containersFile);

      try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(metadataFile),
          SequenceFile.Reader.bufferSize(4096))) {
        Text container = new Text();
        NullWritable dummy = NullWritable.get();

        List<String> containers = new ArrayList<>();
        while (reader.next(container, dummy)) {
          String containerId = container.toString();
          containers.add(containerId);
        }

        nodeToContainersMap.put(nodeLogs.getPath().getName(), containers);
      }
    }

    return nodeToContainersMap;
  }

  public List<FileStatus> getLogsDirsFileStatusListForApp(ApplicationId appId, String appOwner) throws IOException {
    Map<String, List<String>> nodeToContainersMetadata = getLogMetadataForApplication(appId.toString(), appOwner);

    Path rootLocalVolumePath = new Path(YarnDefaultProperties.DEFAULT_MAPR_LOCAL_VOL_PATH);
    String logsPathForLocalVolume = LogAggregationUtils.LOG_PATH_FOR_LOCAL_VOLUME;

    FileSystem fs = FileSystem.get(conf);

    List<FileStatus> fileStatusList = new ArrayList<>();
    for (Map.Entry<String, List<String>> e : nodeToContainersMetadata.entrySet()) {
      Path finalPath = new Path(rootLocalVolumePath, e.getKey() + logsPathForLocalVolume + appOwner + "/" + appId + "/logs");
      try{
        fileStatusList.add(fs.getFileStatus(finalPath));
      } catch (IOException ex){
        LOG.warn(ex);
      }
    }
    return fileStatusList;
  }

  public List<FileStatus> getAppLogsDirsFileStatusListForApp(ApplicationId appId, String appOwner) throws IOException {
    Map<String, List<String>> nodeToContainersMetadata = getLogMetadataForApplication(appId.toString(), appOwner);

    Path rootLocalVolumePath = new Path(YarnDefaultProperties.DEFAULT_MAPR_LOCAL_VOL_PATH);
    String logsPathForLocalVolume = LogAggregationUtils.LOG_PATH_FOR_LOCAL_VOLUME;

    FileSystem fs = FileSystem.get(conf);

    List<FileStatus> fileStatusList = new ArrayList<>();
    for (Map.Entry<String, List<String>> e : nodeToContainersMetadata.entrySet()) {
      Path finalPath = new Path(rootLocalVolumePath, e.getKey() + logsPathForLocalVolume + appOwner + "/" + appId);
      try{
        fileStatusList.add(fs.getFileStatus(finalPath));
      } catch (IOException ex){
        LOG.warn(ex);
      }
    }
    return fileStatusList;
  }

  private Path getPathForApp(String applicationId, String appOwner) {
    return new Path(prefixPath, appOwner + "/" + logMetaDir + "/" + applicationId);
  }
}
