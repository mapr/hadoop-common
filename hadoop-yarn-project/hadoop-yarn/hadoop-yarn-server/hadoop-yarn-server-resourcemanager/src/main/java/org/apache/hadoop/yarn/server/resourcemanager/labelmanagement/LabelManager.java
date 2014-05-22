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
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.dev.eval.Expression;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

public class LabelManager extends AbstractService {

  private static final Log LOG = LogFactory.getLog(LabelManager.class);
  
  private long lastModified = 0;
  public static Pattern regex = Pattern.compile("[^\\s,\"']+|\"([^\"]*)\"|'([^']*)'");
  public static Pattern alpha_num = Pattern.compile("^[A-Za-z0-9_ ]+$");
  public static Pattern keywords = Pattern.compile("^int$|^abs$|^pow$");
  public static final String JT_NODE_LABELS_FILE = 
      "mapreduce.jobtracker.node.labels.file";
  public static final String JT_NODE_LABELS_MONITOR_INTERVAL = 
      "mapreduce.jobtracker.node.labels.monitor.interval";

  private Map<String,List<String>> nodeNotifierLabels = new HashMap<String,List<String>>();
  private Map<String, BigDecimal> labelEvalFillers = new HashMap<String, BigDecimal>();
  private FileSystem fs;

  private Path labelFile = null;
  private long labelManagerMonitorInterval = 2*60*1000;
  
  private static LabelManager s_instance = new LabelManager();
  
  private Timer timer;
  private FileMonitor ttask;
  
  private LabelManager() {
    super(LabelManager.class.getName());
    timer = new Timer();
    ttask = new FileMonitor();
  }
  
  public static LabelManager getInstance() {
    return s_instance;
  }
  
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    fs = FileSystem.get(conf);
    
    String labelFilePath = conf.get(JT_NODE_LABELS_FILE, null);
    if (labelFilePath != null) {
      this.labelFile = new Path(labelFilePath);
      if (!fs.exists(labelFile)) {
          LOG.warn("Could not find node label file " + fs.makeQualified(labelFile) + 
                   ". Node labels will not be set.");
          this.labelFile = null;
      }
      this.labelManagerMonitorInterval = conf.getLong(JT_NODE_LABELS_MONITOR_INTERVAL, 
                                             2*60*1000);
    } else {
      labelFile = null;
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {  
    if (labelFile != null) {
      timer.scheduleAtFixedRate(ttask, 0, labelManagerMonitorInterval);
    }
    
  }
  
  @Override
  protected void serviceStop() throws Exception {
   this.timer.cancel();
  }

  private class FileMonitor extends TimerTask {

    @Override
    public void run() {
        try {
          // check if file is modified
          if (fileChanged()) {
            loadAndApplyLabels();
          }
        } catch (Exception e) {
          LOG.error("LabelManager Thread got exception: " +
                    StringUtils.stringifyException(e) + ". Ignoring...");
        }
      }
  }
  
  private boolean fileChanged() throws IOException {
    FileStatus labelFileStatus = null;
    if (fs.exists(labelFile)) {
      labelFileStatus = fs.getFileStatus(labelFile);
      if (labelFileStatus != null)  {
        // first time load the file
        if (lastModified == 0 || 
            lastModified < labelFileStatus.getModificationTime()) {
          lastModified = labelFileStatus.getModificationTime();
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Read a line from file and parse node identifier and labels.
   */
  void loadAndApplyLabels() throws IOException {
    if (fs.exists(labelFile)) {
      FSDataInputStream input = fs.open(labelFile);
      BufferedReader sin = new BufferedReader(new InputStreamReader(input));
      String str = null;
      Map<String,List<String>> nodeNotifierLabelsTmp = new HashMap<String,List<String>>();
      Map<String, BigDecimal> labelEvalFillersTmp = new HashMap<String, BigDecimal>();
      int lineno = 0;
      String nodeIdentifier;
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
        } else {
          // its a glob support only * and ?
          nodeIdentifier = tokens[0].replaceAll("\\*",".*");
          nodeIdentifier = nodeIdentifier.replaceAll("\\?",".");
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
        nodeNotifierLabelsTmp.put(nodeIdentifier, nodeLabels);
      }
      // swap needs to take place here
      synchronized ( nodeNotifierLabels ) {
        nodeNotifierLabels.clear();
        nodeNotifierLabels.putAll(nodeNotifierLabelsTmp);
      }
      synchronized (labelEvalFillers) {
        labelEvalFillers.clear();
        labelEvalFillers.putAll(labelEvalFillersTmp);
      }
      nodeNotifierLabelsTmp.clear();
      labelEvalFillersTmp.clear();
      nodeNotifierLabelsTmp = null; // hint to GC it
      labelEvalFillersTmp = null;
    }
  }

  public List<String> getLabelsForNode(String node) {
    Map<String,List<String>> nodeNotifierLabelsTmp = new HashMap<String,List<String>>();
    synchronized ( nodeNotifierLabels ) {
      nodeNotifierLabelsTmp.putAll(nodeNotifierLabels);
    }
    
    List<String> nodeLabels = new ArrayList<String>();
    for ( Map.Entry<String, List<String>> entry : nodeNotifierLabelsTmp.entrySet()) {
      String nodeIdentifier = entry.getKey();
      if ( node.matches(nodeIdentifier)) {
        nodeLabels.addAll(entry.getValue());
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
    return nodeLabels;
  }

  // TODO should not evaluate > 1 per app - may be move it to AppInfo
  public Expression constructAppLabel(String appLabelStr) throws IOException {
    if ( appLabelStr == null ) {
      return null;
    }
    Expression appLabelExpression;
    
    String appLabelExpressionString = appLabelStr.replaceAll("^\"|\"$", "");
    // check if app could be scheduled anywhere in cluster.
    if ("*".equals(appLabelExpressionString) || 
        "all".equals(appLabelExpressionString)) {
      appLabelExpression = null;
    } else {
      try {
        // create an expression, set fillEmptyValues to true and default value 0
        appLabelExpression = new Expression(appLabelExpressionString);
      } catch (Throwable t) {
        LOG.warn("Invalid label format " + appLabelExpressionString + 
                 " Error " + t);
        return null;
      }
    }
    
    
    return appLabelExpression;
  }
  
  public Expression constructAppLabel(Queue.QueueLabelPolicy policy,
                                      Expression appLabelExpression,
                                      Expression queueLabelExpression) {
    if (Queue.QueueLabelPolicy.AND.equals(policy)) {
      if (queueLabelExpression != null && appLabelExpression != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
                              ") && (" + appLabelExpression.toString() + ")");
      }
      if (queueLabelExpression == null) {
        return appLabelExpression;
      } else {
        return queueLabelExpression;
      }
    } else if (Queue.QueueLabelPolicy.OR.equals(policy)) {
      if (queueLabelExpression != null && appLabelExpression != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
                              ") || (" + appLabelExpression.toString() + ")");
      }
      if (queueLabelExpression == null) {
        return appLabelExpression;
      } else {
        return queueLabelExpression;
      }
    } else if (Queue.QueueLabelPolicy.PREFER_QUEUE.equals(policy)) {
      return queueLabelExpression;
    } else if (Queue.QueueLabelPolicy.PREFER_APP.equals(policy)) {
      return appLabelExpression;
    }
    // wrong/no policy? return back app label
    return appLabelExpression;
  }
  
  public boolean evaluateAppLabelAgainstNode(String node, Expression finalAppLabelExp) 
    throws IOException {
    if ( finalAppLabelExp == null ) {
      return false;
    }
    List<String> nodeLabels = getLabelsForNode(node);
    if ( nodeLabels == null || nodeLabels.isEmpty() ) {
      return false;
    }
    Map<String, BigDecimal> labelEvalFillersTmp = 
        new HashMap<String, BigDecimal>();
    synchronized (labelEvalFillers) {
      labelEvalFillersTmp.putAll(labelEvalFillers);
    }
    
    for ( String label : nodeLabels) {
      labelEvalFillersTmp.put(label, BigDecimal.valueOf(1l));
    }
    try {
      BigDecimal retValue = finalAppLabelExp.eval(labelEvalFillersTmp);
      return (retValue.intValue() == 0 ) ? false : true;
    } catch (Throwable t ) {
      LOG.warn("Exception while evaluating: " + finalAppLabelExp , t);
      throw new IOException("Exception while evaluating: " + finalAppLabelExp);
    }
  }

  public boolean canNodeBeEvaluated(String node, Expression finalAppLabelExp) {
    if ( finalAppLabelExp == null ) {
      return false;
    }
    List<String> nodeLabels = getLabelsForNode(node);
    if ( nodeLabels == null || nodeLabels.isEmpty() ) {
      return false;
    }
    return true;
  }

  public Path getLabelFile() {
    return labelFile;
  }
}
