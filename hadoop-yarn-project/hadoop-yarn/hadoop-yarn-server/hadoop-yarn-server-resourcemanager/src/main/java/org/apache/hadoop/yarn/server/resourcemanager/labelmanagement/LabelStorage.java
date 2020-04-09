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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.java.dev.eval.Expression;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

@Private
/**
 * This is a class responsible for 
 * loading
 * storing 
 * refreshing
 * of Label related information
 *
 */
public final class  LabelStorage {

  private static final Log LOG = LogFactory.getLog(LabelStorage.class);

  public static final Pattern regex = Pattern.compile("[^\\s,\"']+|\"([^\"]*)\"|'([^']*)'");
  public static final Pattern alpha_num = Pattern.compile("^[A-Za-z0-9_ ]+$");
  public static final Pattern keywords = Pattern.compile("^int$|^abs$|^pow$");

  private FileSystem fs;

  private Path labelFile = null;

  private static LabelStorage s_instance = new LabelStorage();

  /**
   * Map of node Regex to list of labels associated with it
   * example:
   * perfnode.*  blue, red green
   * This Map has to change atomically during reload and therefore 
   * had to be protected by lock versus be ConcurrentHashMap
   */
  private Map<String, List<String>> nodeExpressionLabels = new HashMap<String,List<String>>();

  /**
   * Duplicate of nodeExpressionLabels map without glob pre-processing.
   * Used to remove and replace existing labels.
   */
  private Map<String, List<String>> nodeNoGlobExpressionLabels = new HashMap<>();

  /**
   * Map to hold mapping between real node and labels - this will ensure
   * that if we evaluated this node already to the set of labels
   * there is no need to reevaluate it again unless content of the file with labels
   * changed
   */
  private Map<String, Set<String>> nodeToLabelsMap = new ConcurrentHashMap<String,Set<String>>();

  /**
   * Set of nodes that evaluated to no label (i.e. rack-local, or other nodes)
   * so if this node was evaluated to no labels there is no point of reevaluating again
   * unless content of the file with labels changed
   */
  private Set<String> nodeNoMatchers = Collections.newSetFromMap(
                                        new ConcurrentHashMap<String, Boolean>());
  /**
   * Map between label and number (0) for logical evaluation of expression
   * this map could be considered a set of all labels defined in node to labels file
   * mapping. It is used as a placeholder for concrete evaluation
   */
  private Map<String, BigDecimal> labelEvalFillers = new HashMap<String, BigDecimal>();
  
  /**
   * All labels defined on a cluster
   */
  private final Set<Expression> labels = new HashSet<>();

  private LabelStorage() {
    
  }

  public static LabelStorage getInstance() {
    return s_instance;
  }
  
  void storageInit(FileSystem fs, Path labelFile) {
    this.fs = fs;
    this.labelFile = labelFile;
  }
  
  /**
   * Read a line from file and parse node identifier and labels.
   */
  @Private
  void loadAndApplyLabels(Configuration conf) throws IOException {
    String labelFilePath = conf.get(LabelManager.NODE_LABELS_FILE, null);
    labelFile = labelFilePath == null ? null : new Path(labelFilePath);
    
    if (labelFile == null || !fs.exists(labelFile) || fs.getContentSummary(labelFile).getLength() == 0) {
      synchronized (nodeExpressionLabels) {
        synchronized (nodeNoGlobExpressionLabels) {
          nodeExpressionLabels.clear();
          nodeNoGlobExpressionLabels.clear();
        }
      }
      synchronized (labels) {
        labels.clear();
      }
      nodeToLabelsMap.clear();
      LOG.info("Labels file is empty or does not exist: " + labelFile +". Labels are cleaned up.");
      
      return;
    }
    FSDataInputStream input = fs.open(labelFile);
    BufferedReader sin = new BufferedReader(new InputStreamReader(input));
    try {
      String str = null;
      Map<String,List<String>> nodeNotifierLabelsTmp = new HashMap<String,List<String>>();
      Map<String,List<String>> nodeNoGlobNotifierLabelsTmp = new HashMap<String,List<String>>();
      Map<String, BigDecimal> labelEvalFillersTmp = new HashMap<String, BigDecimal>();
      int lineno = 0;
      String nodeIdentifier;
      String noGlobNodeIdentifier;
      while (true) {
        // scan each line
        str = sin.readLine();
        if (str == null)  break;
        lineno++;
        String []tokens = str.split("\\s+", 2);
        // min 2 
        if (tokens.length != 2) {
          LOG.warn("Wrong format in node label file -> " + lineno + ":" + str);
          continue;
        }
        if (tokens[0].startsWith("/") && tokens[0].endsWith("/")) {
          nodeIdentifier = tokens[0].replaceAll("^\\/|\\/$", "");
          noGlobNodeIdentifier = tokens[0].replaceAll("^\\/|\\/$", "");
        } else {
          // its a glob support only * and ?
          nodeIdentifier = globSupport(tokens[0]);
          noGlobNodeIdentifier = tokens[0];
        }
        List<String> nodeLabels = new ArrayList<String>();
        Matcher regexMatcher = regex.matcher(tokens[1]);
        while (regexMatcher.find()) {
          String term;
          if (regexMatcher.group(1) != null) {
            term = regexMatcher.group(1);
          } else if (regexMatcher.group(2) != null) {
            term = regexMatcher.group(2);
          } else {
            term = regexMatcher.group();
          }
          if (term != null 
              && alpha_num.matcher(term).matches() 
              && !keywords.matcher(term).matches()) {
            nodeLabels.add(term);
            labelEvalFillersTmp.put(term, BigDecimal.ZERO);
          } else {
            LOG.warn("Invalid node label: '" + term + "'");
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("nodeIdentifier :" + nodeIdentifier + " labels :" + nodeLabels);
        }
        nodeNoGlobNotifierLabelsTmp.put(noGlobNodeIdentifier, nodeLabels);
        nodeNotifierLabelsTmp.put(nodeIdentifier, nodeLabels);
      }

      // swap needs to take place here
      // No interference between multiple threads trying to modify state
      synchronized ( nodeExpressionLabels ) {
        synchronized ( labelEvalFillers ) {
          synchronized (nodeNoGlobExpressionLabels) {
            nodeExpressionLabels.clear();
            nodeNoGlobExpressionLabels.clear();
            nodeExpressionLabels.putAll(nodeNotifierLabelsTmp);
            nodeNoGlobExpressionLabels.putAll(nodeNoGlobNotifierLabelsTmp);

            labelEvalFillers.clear();
            labelEvalFillers.putAll(labelEvalFillersTmp);
            // restart process of filling up following Collections
            nodeToLabelsMap.clear();
            nodeNoMatchers.clear();
          }

        }
      }
      updateClusterLabels();

      nodeNoGlobNotifierLabelsTmp.clear();
      nodeNotifierLabelsTmp.clear();
      labelEvalFillersTmp.clear();
      nodeNotifierLabelsTmp = null; // hint to GC it
      nodeNoGlobNotifierLabelsTmp = null;
      labelEvalFillersTmp = null;
    } finally {
      sin.close();
    }
  }

  Set<String> getLabelsForNode(String node) {
    
    Set<String> labelsForNode = nodeToLabelsMap.get(node.toLowerCase());
    if ( labelsForNode != null ) {
      return labelsForNode;
    }
    
    // no need to proceed
    if ( nodeNoMatchers.contains(node.toLowerCase())) {
      return null;
    }
    
    Map<String,List<String>> nodeNotifierLabelsTmp = new HashMap<String,List<String>>();
    synchronized ( nodeExpressionLabels ) {
      nodeNotifierLabelsTmp.putAll(nodeExpressionLabels);
    }
    
    Set<String> nodeLabels = new HashSet<String>();
    for ( Map.Entry<String, List<String>> entry : nodeNotifierLabelsTmp.entrySet()) {
      String nodeIdentifier = entry.getKey();
      if ( node.matches(nodeIdentifier)) {
        nodeLabels.addAll(entry.getValue());
        Set<String> listFromMap = nodeToLabelsMap.get(node.toLowerCase());
        if ( listFromMap == null ) {
          nodeToLabelsMap.put(node.toLowerCase(), new HashSet<String>(entry.getValue()));
        } else {
          listFromMap.addAll(entry.getValue());
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding labels for node: " + node + ", labels: " + nodeLabels);
        }
      } else {
        if (LOG.isDebugEnabled()) { 
          LOG.debug("Identifier not matching setLabel node: '" + node +
              "' identifier: '" + nodeIdentifier+"'");
        }
      }
    }
    nodeNotifierLabelsTmp.clear();
    nodeNotifierLabelsTmp = null; // hint for GC
    
    if ( nodeLabels.isEmpty() ) {
      nodeNoMatchers.add(node.toLowerCase());
      return null;
    }
    return nodeLabels;
  }

  List<NodeToLabelsList> getLabelsForAllNodes(boolean globSupport) {
    Map<String,List<String>> nodeNotifierLabelsTmp = new HashMap<String,List<String>>();

    if (globSupport) {
      synchronized (nodeExpressionLabels) {
        nodeNotifierLabelsTmp.putAll(nodeExpressionLabels);
      }
    } else {
      synchronized (nodeNoGlobExpressionLabels) {
        nodeNotifierLabelsTmp.putAll(nodeNoGlobExpressionLabels);
      }
    }

    List<NodeToLabelsList> nodeToLabelsList= new ArrayList<NodeToLabelsList>();

    for (Map.Entry<String, List<String>> entry : nodeNotifierLabelsTmp.entrySet()) {
      NodeToLabelsList singleNodeToLabelsList =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(NodeToLabelsList.class);
      singleNodeToLabelsList.setNode(entry.getKey());
      singleNodeToLabelsList.setNodeLabel(entry.getValue());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding labels for node: " + entry.getKey() + ", labels: " + entry.getValue());
      }
      nodeToLabelsList.add(singleNodeToLabelsList);
    }

    nodeNotifierLabelsTmp.clear();
    nodeNotifierLabelsTmp = null; // hint for GC
    return nodeToLabelsList;
  }

  Map<String, BigDecimal> getFillers() {
    Map<String, BigDecimal> labelEvalFillersTmp = 
        new HashMap<String, BigDecimal>();
    synchronized (labelEvalFillers) {
      labelEvalFillersTmp.putAll(labelEvalFillers);
    }
    return labelEvalFillersTmp;
  }

  private void updateClusterLabels() {
    Set<Expression> allLabels = new HashSet<>();
    Expression labelExpression = null;
    List<NodeToLabelsList> labelsForAllNodes = getLabelsForAllNodes(true);
    for (NodeToLabelsList nodeToLabelsList : labelsForAllNodes) {
      for (String label : nodeToLabelsList.getNodeLabel()) {
        try {
          labelExpression = LabelManager.getInstance().getEffectiveLabelExpr(label);
        } catch (IOException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception while trying to evaluate label expression " + labelExpression, e);
          }
          continue;
        }
        allLabels.add(labelExpression);
      }
    }

    synchronized (labels) {
      labels.clear();
      labels.addAll(allLabels);
    }
  }
  
  public Set<Expression> getLabels() {
    return new HashSet<>(labels);
  }

  public void addNodeLabels(Map<String, List<String>> newLabels) throws IOException, YarnException {
    if (null == labelFile) {
      LOG.error("Label-based scheduling is not enabled. Specify the path to the node.labels file");
      return;
    }

    Map<String, List<String>> modifiedLabels = new HashMap<>(nodeNoGlobExpressionLabels);

    Set<String> hostnames = new HashSet<>(newLabels.keySet());
    for (String hostname : hostnames) {
      if (modifiedLabels.containsKey(hostname)) {
        if (newLabels.get(hostname).equals(modifiedLabels.get(hostname))) {
          LOG.warn("Labels for node [" + hostname + "] already defined! Ignoring.");
        } else {
          modifiedLabels.get(hostname).addAll(newLabels.get(hostname));
          modifiedLabels.replace(hostname,
              modifiedLabels.get(hostname).stream().distinct().collect(Collectors.toList()));
        }
        newLabels.remove(hostname);
      }
    }
    modifiedLabels.putAll(newLabels);
    if (!modifiedLabels.isEmpty()) {
      List<String> newLabelsList = packNodeLabelsToList(modifiedLabels);
      writeToFile(newLabelsList);
    }
  }

  private void writeToFile(List<String> nodeLabels) throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(labelFile, true)));
    for (String label : nodeLabels) {
      writer.write(label);
      writer.newLine();
    }
    writer.flush();
    writer.close();
  }

  public void replaceLabelsOnNode(Map<String, Map<String, String>> labelsForReplace) throws IOException {
    Map<String, List<String>> modifiedLabels = new HashMap<>(nodeNoGlobExpressionLabels);

    fs.delete(labelFile, false);
    nodeExpressionLabels.clear();
    try {
      for (String nodeName : labelsForReplace.keySet()) {
        if (!nodeName.equals("*") && !modifiedLabels.containsKey(nodeName)) {
          LOG.error("Hostname [" + nodeName + "] does not exists in current configuration!");
          continue;
        }

        if (nodeName.equals("*")) {
          modifiedLabels.forEach((hostname, labels) -> labels.replaceAll(label -> labelsForReplace.get("*").getOrDefault(label, label)));
        } else {
          List<String> currentLabels = modifiedLabels.get(nodeName);
          currentLabels.replaceAll(label -> labelsForReplace.get(nodeName).getOrDefault(label, label));
        }
      }
      writeToFile(packNodeLabelsToList(modifiedLabels));
    } catch (Exception e) {
      writeToFile(packNodeLabelsToList(nodeNoGlobExpressionLabels));
      throw e;
    }

  }

  public void removeNodeLabels(Map<String, List<String>> oldLabels) throws IOException {
    Map<String, List<String>> modifiedLabels = new HashMap<>(nodeNoGlobExpressionLabels);

    fs.delete(labelFile, false);
    nodeExpressionLabels.clear();

    try {
      if ((oldLabels.keySet().size() == 1 && oldLabels.containsKey("*")) &&
          (oldLabels.get("*") == null || oldLabels.get("*").isEmpty())) {
        fs.create(labelFile, true);
        return;
      } else if (oldLabels.keySet().size() == 1 && oldLabels.containsKey("*")) {
        modifiedLabels.values().forEach(list -> list.removeAll(oldLabels.get("*")));
        clearEmptyNodes(modifiedLabels);
      } else {
        Set<String> hostnames = new HashSet<>(oldLabels.keySet());
        for (String hostname : hostnames) {
          if (modifiedLabels.containsKey(hostname)) {
            if (oldLabels.get(hostname).size() == 1 && oldLabels.get(hostname).contains("*")) {
              modifiedLabels.remove(hostname);
            } else {
              modifiedLabels.get(hostname).removeAll(oldLabels.get(hostname));
            }
          }
        }
        clearEmptyNodes(modifiedLabels);
      }
      if (!modifiedLabels.isEmpty()) {
        writeToFile(packNodeLabelsToList(modifiedLabels));
      }
    } catch (IOException e) {
      writeToFile(packNodeLabelsToList(nodeNoGlobExpressionLabels));
      throw e;
    }
  }

  private Map<String, List<String>> clearEmptyNodes(Map<String, List<String>> labels) {
    Set<String> hostnames = new HashSet<>(labels.keySet());
    for (String hostname : hostnames) {
      if (labels.get(hostname).isEmpty()) {
        labels.remove(hostname);
      }
    }
    return labels;
  }

  private List<String> packNodeLabelsToList(Map<String, List<String>> map) {
    List<String> nodeLabels = new ArrayList<>();

    map.forEach((hostname, labels) -> {
      StringBuilder line = new StringBuilder(hostname);
      for (String label : labels) {
        line.append(" ").append(label);
      }
      nodeLabels.add(line.toString());
    });

    return nodeLabels;
  }

  private String globSupport(String str) {
    return str.replaceAll("\\*",".*").replaceAll("\\?",".");
  }

}
