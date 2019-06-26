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
package org.apache.hadoop.yarn.server.resourcemanager.labelmanagement;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import net.java.dev.eval.Expression;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

/**
 * LabelManager class is a service that provides ability to deal with
 * label based scheduling - namely helps to schedule containers on 
 * predefined set of nodes
 * This service's responsibility include:
 * 1. Reading file with the labels from DFS and periodically refreshing it
 *    if it was changed
 * 2. Create label expressions based String provided
 * 3. Create label expressions based on combination of Label Expressions and Policies
 * 4. Evaluate expression for a node to determine whether the node in question
 *    has needed labels or not.   
 * 
 */
public class LabelManager {

  private static final Log LOG = LogFactory.getLog(LabelManager.class);
  
  private long lastModified = 0;
  public static final String NODE_LABELS_FILE = 
      "node.labels.file";
  public static final String NODE_LABELS_MONITOR_INTERVAL = 
      "node.labels.monitor.interval";
  public static final long DEFAULT_RELOAD_INTERVAL = 2*60*1000l;

  private FileSystem fs;
  
  private Configuration config;

  private Path labelFile = null;
  private long labelManagerMonitorInterval = DEFAULT_RELOAD_INTERVAL;
  
  private static LabelManager s_instance = new LabelManager();
  
  private Timer timer;
  private FileMonitor ttask;
  private volatile boolean isServiceEnabled;

  private LabelStorage storage = LabelStorage.getInstance();
  
  private LabelManager() {
  }
  
  public static LabelManager getInstance() {
    return s_instance;
  }
  
  void serviceInit(Configuration conf) throws Exception {
    setConfig(conf);
    fs = FileSystem.get(conf);

    String labelFilePath = conf.get(NODE_LABELS_FILE, null);
    if (labelFilePath != null) {
      this.labelFile = new Path(labelFilePath);
      if (!fs.exists(labelFile)) {
          LOG.warn("Could not find node label file " + fs.makeQualified(labelFile) + 
                   ". Node labels will not be set.");
          this.labelFile = null;
      }
      this.labelManagerMonitorInterval = conf.getLong(NODE_LABELS_MONITOR_INTERVAL, 
          DEFAULT_RELOAD_INTERVAL);
    } 
    storage.storageInit(fs, labelFile);
  }
  
  void serviceStart() throws Exception {  
    if (labelFile != null) {
      timer = new Timer();
      ttask = new FileMonitor();
      timer.scheduleAtFixedRate(ttask, 0, labelManagerMonitorInterval);
      isServiceEnabled = true;
    }
  }
  
  void serviceStop() throws Exception {
    if ( timer != null ) {
      timer.cancel();
    }
  }

  public void setConfig(Configuration conf) {
    this.config = conf;
  }

  /**
   * 
   * @return whether service is enabled and worth using it
   */
  public boolean isServiceEnabled() {
    return isServiceEnabled;
  }
  
  private class FileMonitor extends TimerTask {

    @Override
    public void run() {
        try {
          // check if file is modified
          if (fileChanged()) {
            storage.loadAndApplyLabels(config);
          }
        } catch (Exception e) {
          LOG.error("LabelManager Thread got exception: " +
                    StringUtils.stringifyException(e) + ". Ignoring...");
        }
      }
  }
  
  private boolean fileChanged() throws IOException {
    FileStatus labelFileStatus = null;
    if (!fs.exists(labelFile)) {
      return false;
    }
    labelFileStatus = fs.getFileStatus(labelFile);
    if (labelFileStatus != null)  {
      // first time load the file
      if (lastModified == 0 || 
          lastModified < labelFileStatus.getModificationTime()) {
        lastModified = labelFileStatus.getModificationTime();
        return true;
      }
    }
    return false;
  }

  /**
   * Read a line from file and parse node identifier and labels.
   */
  @Private
  public void refreshLabels(Configuration conf) throws IOException {
    storage.loadAndApplyLabels(conf);
  }

  public Set<String> getLabelsForNode(String node) {
    return storage.getLabelsForNode(node);
  }

  public List<NodeToLabelsList> getLabelsForAllNodes() {
    return storage.getLabelsForAllNodes();
  }

  public Set<Expression> getLabels() {
    return storage.getLabels();
  }

  /**
   * This method converts data from LabelManager data representation to default
   * apache structure. If labelsToAdd empty or null, this method will return data
   * about all label and corresponding nodes.
   * @param labelsToAdd labels needed to add to result.
   * @return Map of node labels and appropriate nodes
   */
  public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labelsToAdd) {
    Map<String, Set<NodeId>> labelsToNodes = new HashMap<>();

    for (NodeToLabelsList n : storage.getLabelsForAllNodes()) {
      NodeId nodeId = NodeId.newInstance(n.getNode(), 0);
      List<String> nodeLabels = n.getNodeLabel();

      for (String label : nodeLabels) {
        if ((labelsToAdd == null || labelsToAdd.isEmpty()) ||  (labelsToAdd != null && labelsToAdd.contains(label))) {
          Set<NodeId> nodeIds = labelsToNodes.get(label);
          if (nodeIds == null) {
            nodeIds = new HashSet<>();
            nodeIds.add(nodeId);
            labelsToNodes.put(label, nodeIds);
          } else {
            nodeIds.add(nodeId);
            labelsToNodes.remove(label);
            labelsToNodes.put(label, nodeIds);
          }
        }
      }
    }
    return labelsToNodes;
  }


  /**
   * This method converts data from LabelManager data representation to default apache structure.
   * @return Map of nodes and appropriate labels
   */
  public Map<NodeId, Set<String>> getNodeToLabels() {
    Map<NodeId, Set<String>> nodeToLabels = new HashMap<>();

    for (NodeToLabelsList n : storage.getLabelsForAllNodes()) {
      NodeId nodeId = NodeId.newInstance(n.getNode(), 0);
      Set<String> nodeLabels = new HashSet<>(n.getNodeLabel());

      nodeToLabels.put(nodeId, nodeLabels);
    }
    return nodeToLabels;
  }
  
  public Expression getEffectiveLabelExpr(String labelStr) throws IOException {
    return LabelExpressionHandlingHelper.getEffectiveLabelExpr(labelStr);
  }
  
  public Expression constructAppLabel(Queue.QueueLabelPolicy policy,
                                      Expression appLabelExpression,
                                      Expression queueLabelExpression) {
    return LabelExpressionHandlingHelper.constructAppLabel(policy, 
        appLabelExpression, 
        queueLabelExpression);
  }
  
  public LabelApplicabilityStatus isNodeApplicableForApp(String node, Expression finalAppLabelExp) 
    throws IOException {
    return LabelExpressionHandlingHelper.isNodeApplicableForApp(node, finalAppLabelExp);
  }

  public List<String> getNodesForLabel(Expression label) throws IOException {
    return LabelExpressionHandlingHelper.getNodesForLabel(label);
  }

  public Path getLabelFile() {
    return labelFile;
  }
  
  public static enum LabelApplicabilityStatus {
    NOT_APPLICABLE,
    NODE_HAS_LABEL,
    NODE_DOES_NOT_HAVE_LABEL;
  }
}
