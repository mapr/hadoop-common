package org.apache.hadoop.yarn.server.nodemanager.containermanager.nodelocallogaggregation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.util.List;

public class NodeLocalMetadataWriter {
  private static final Log LOG = LogFactory.getLog(NodeLocalMetadataWriter.class);

  private final String prefixPath;
  private final String logMetaDir;
  private final String containersList;
  private final Configuration conf;
  private FileSystem fs;
  private boolean prefixPathExists;

  public NodeLocalMetadataWriter(Configuration conf) {
    this.conf = conf;
    this.prefixPath = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR);
    this.logMetaDir = conf.get(YarnConfiguration.NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME,
        YarnConfiguration.DEFAULT_NODE_LOCAL_AGGREGATION_METADATA_DIR_NAME);
    this.containersList = conf.get(YarnConfiguration.NODE_LOCAL_AGGREGATION_METADATA_FILENAME,
        YarnConfiguration.DEFAULT_NODE_LOCAL_AGGREGATION_METADATA_FILENAME);
    prefixPathExists = false;
  }

  public void write(ApplicationId applicationId, NodeId nodeId, List<ContainerId> containers, String appOwner) throws IOException {
    if (!prefixPathExists) {
      try {
        fs = FileSystem.get(conf);
        if (!fs.exists(new Path(prefixPath))) {
          fs.mkdirs(new Path(prefixPath));
        }
      } catch (IOException e) {
        LOG.warn("Can't create parent directory for metadata");
      }
      prefixPathExists = true;
    }
    NodeLocalApplicationLogMetadata info = new NodeLocalApplicationLogMetadata();
    info.setApplicationId(applicationId.toString());
    info.setNodeId(nodeId.getHost());
    info.setAppOwner(appOwner);
    info.setContainers(containers);
    writeInternal(info);
  }

  private void checkOrCreatePath(Path path) throws IOException {
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void writeInternal(NodeLocalApplicationLogMetadata info) throws IOException {
    Path pathToMetadataDir = getPathForApp(info.getApplicationId(), info.getAppOwner());
    Path pathToMetadataNodeDir = new Path(pathToMetadataDir, info.getNodeId());
    checkOrCreatePath(pathToMetadataNodeDir);

    Path containerToNodeMetadataPath = new Path(pathToMetadataNodeDir, containersList);

    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(containerToNodeMetadataPath),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(NullWritable.class));

    writeContainerNodePairs(writer, info.getContainers());

    writer.close();
  }

  private void writeContainerNodePairs(SequenceFile.Writer writer, List<ContainerId> containers) throws IOException {
    for (ContainerId containerId : containers) {
      writer.append(new Text(containerId.toString()), NullWritable.get());
    }
  }

  private Path getPathForApp(String applicationId, String appOwner) {
    return new Path(prefixPath, appOwner + "/" + logMetaDir + "/" + applicationId);
  }
}
