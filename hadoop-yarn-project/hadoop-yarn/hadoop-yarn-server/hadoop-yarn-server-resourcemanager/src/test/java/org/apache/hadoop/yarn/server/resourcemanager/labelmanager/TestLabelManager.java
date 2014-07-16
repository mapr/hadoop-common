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
package org.apache.hadoop.yarn.server.resourcemanager.labelmanager;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import net.java.dev.eval.Expression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManagementService;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManager;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManager.LabelApplicabilityStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLabelManager {

  static Configuration conf;
  static FileSystem fs;
  static LabelManagementService lbS;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.set(LabelManager.NODE_LABELS_FILE, "/tmp/labelFile");
    // set label refresh interval to 5 seconds
    conf.setLong(LabelManager.NODE_LABELS_MONITOR_INTERVAL, 
        5*1000);
    
    conf.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem");
    conf.set("fs.default.name", "file:///");
    fs = FileSystem.getLocal(conf);
    
    FSDataOutputStream fsout = fs.create(new Path("/tmp/labelFile"));
    fsout.writeBytes("/perfnode200.*/ big, \"Production Machines\"");
    fsout.writeBytes("\n");
    fsout.writeBytes("/perfnode203.*/ big, 'Development Machines'");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode15* good");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode201* slow");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode204* good, big");
    fsout.writeBytes("\n");
    fsout.writeBytes("/node-.+lab/     Fast");  
    fsout.writeBytes("\n");
    fsout.writeBytes("node-2*      Slow");
    fsout.writeBytes("\n");
    fsout.close();

    
    lbS = new LabelManagementService();
    lbS.init(conf);
    lbS.start();
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    //LabelManager lb = LabelManager.getInstance();
    lbS.stop();
    assertFalse(lbS.getServiceState() != Service.STATE.STOPPED);
    fs.delete(new Path("/tmp/labelFile"), false);
  }

  @Before
  public void setUp() throws Exception {
    FSDataOutputStream fsout = fs.create(new Path("/tmp/labelFile"));
    fsout.writeBytes("/perfnode200.*/ big, \"Production Machines\"");
    fsout.writeBytes("\n");
    fsout.writeBytes("/perfnode203.*/ big, 'Development Machines'");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode15* good");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode1* right, good, fantastic");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode201* slow");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode204* good, big");
    fsout.writeBytes("\n");
    fsout.writeBytes("/node-.+lab/     Fast");  
    fsout.writeBytes("\n");
    fsout.writeBytes("node-2*      Slow");
    fsout.writeBytes("\n");
    fsout.close();
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(new Path("/tmp/labelFile"), false);
  }

  @Test (timeout=10000)
  public void testLabelManager() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue("/tmp/labelFile".equalsIgnoreCase(labelFile.toString()));
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
    Thread.sleep(1000l);
    Set<String> labels = lb.getLabelsForNode("perfnode151.perf.lab");
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("good"));
    assertTrue(labels.contains("right"));
    assertTrue(labels.contains("fantastic"));
    
    labels = lb.getLabelsForNode("perfnode200.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("big"));
    assertTrue(labels.contains("Production Machines"));

    labels = lb.getLabelsForNode("perfnode203.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("big"));
    assertTrue(labels.contains("Development Machines"));

    labels = lb.getLabelsForNode("node-33.lab");
    assertNotNull(labels);
    assertEquals(1, labels.size());
    assertTrue(labels.contains("Fast"));

    labels = lb.getLabelsForNode("node-28.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("Slow"));
    assertTrue(labels.contains("Fast"));

    labels = lb.getLabelsForNode("node-28.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("Slow"));
    assertTrue(labels.contains("Fast"));

    labels = lb.getLabelsForNode("perfnode01.lab");
    assertNull(labels);
    
    labels = lb.getLabelsForNode("perfnode01.lab");
    assertNull(labels);
    
    labels = lb.getLabelsForNode("perfnode10.lab");
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("good"));
    assertTrue(labels.contains("right"));
    assertTrue(labels.contains("fantastic"));

    labels = lb.getLabelsForNode("perfnode10.lab");
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("good"));
    assertTrue(labels.contains("right"));
    assertTrue(labels.contains("fantastic"));

  }
  
  @Test
  public void testlabelExpressioncreation() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue("/tmp/labelFile".equalsIgnoreCase(labelFile.toString()));
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
    Thread.sleep(1000l);

    Expression expr = lb.getEffectiveLabelExpr("good && big");
    assertEquals("(good&&big)", expr.toString());
  }
  
  @Test
  public void testAllLabelQueuePolicyExpression() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue("/tmp/labelFile".equalsIgnoreCase(labelFile.toString()));
    //lb.start();
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
    Thread.sleep(1000l);

    Queue.QueueLabelPolicy policy = Queue.QueueLabelPolicy.AND;
    Expression queueLabelExpression = lb.getEffectiveLabelExpr("good && big");
    Expression appLabelExpression = lb.getEffectiveLabelExpr("good");
    
    Expression finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("((good&&big)&&good)", finalExpr.toString());
    
    policy = Queue.QueueLabelPolicy.OR;
    //Expression queueLabelExpression = lb.constructAppLabel("good && big");
    //Expression appLabelExpression = lb.constructAppLabel("good");
    
    finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("((good&&big)||good)", finalExpr.toString());

    policy = Queue.QueueLabelPolicy.PREFER_APP;
    
    finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("(good)", finalExpr.toString());

    policy = Queue.QueueLabelPolicy.PREFER_QUEUE;
    
    finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("(good&&big)", finalExpr.toString());

    
  }
  
  @Test
  public void testLabelExpressionEvauation() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue("/tmp/labelFile".equalsIgnoreCase(labelFile.toString()));
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
    Thread.sleep(1000l);

    Queue.QueueLabelPolicy policy = Queue.QueueLabelPolicy.AND;
    Expression queueLabelExpression = lb.getEffectiveLabelExpr("good && big");
    Expression appLabelExpression = lb.getEffectiveLabelExpr("good");
    
    Expression finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("((good&&big)&&good)", finalExpr.toString());

    LabelApplicabilityStatus result = lb.isNodeApplicableForApp("perfnode204.qa.lab", finalExpr);
    
    assertTrue(result == LabelApplicabilityStatus.NODE_HAS_LABEL);
    
    result = lb.isNodeApplicableForApp("perfnode203.qa.lab", finalExpr);
    
    assertTrue(result == LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL);
    
    
  }

  @Test (timeout=10000)
  public void testLabelRefresh() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue("/tmp/labelFile".equalsIgnoreCase(labelFile.toString()));
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
    Thread.sleep(1000l);
    Set<String> labels = lb.getLabelsForNode("perfnode151.perf.lab");
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("good"));
    assertTrue(labels.contains("right"));
    assertTrue(labels.contains("fantastic"));
    
    labels = lb.getLabelsForNode("perfnode200.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("big"));
    assertTrue(labels.contains("Production Machines"));

    labels = lb.getLabelsForNode("perfnode203.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("big"));
    assertTrue(labels.contains("Development Machines"));

    labels = lb.getLabelsForNode("node-33.lab");
    assertNotNull(labels);
    assertEquals(1, labels.size());
    assertTrue(labels.contains("Fast"));

    labels = lb.getLabelsForNode("node-28.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("Slow"));
    assertTrue(labels.contains("Fast"));

    fs.delete(new Path("/tmp/labelFile"), false);
    
    FSDataOutputStream fsout = fs.create(new Path("/tmp/labelFile"));
    fsout.writeBytes("/perfnode200.*/ big, \"Prod Machines\"");
    fsout.writeBytes("\n");
    fsout.writeBytes("/perfnode203.*/ small, 'Dev Machines'");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode15* good");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode201* slow");
    fsout.writeBytes("\n");
    fsout.writeBytes("perfnode204* good, big");
    fsout.writeBytes("\n");
    fsout.writeBytes("/node-.+lab/     Fast");  
    fsout.writeBytes("\n");
    fsout.writeBytes("node-2*      Slow");
    fsout.writeBytes("\n");
    fsout.close();

    // make sure file refreshed
    Thread.sleep(6*1000l);
    
    labels = lb.getLabelsForNode("perfnode200.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("big"));
    assertTrue(labels.contains("Prod Machines"));

    labels = lb.getLabelsForNode("perfnode203.abc.qa.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("small"));
    assertTrue(labels.contains("Dev Machines"));

    labels = lb.getLabelsForNode("perfnode151.perf.lab");
    assertNotNull(labels);
    assertEquals(1, labels.size());
    assertTrue(labels.contains("good"));
    
    labels = lb.getLabelsForNode("node-33.lab");
    assertNotNull(labels);
    assertEquals(1, labels.size());
    assertTrue(labels.contains("Fast"));

    labels = lb.getLabelsForNode("node-28.lab");
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("Slow"));
    assertTrue(labels.contains("Fast"));

  }
  
  @Test
  public void testBadLabels() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    
    Queue.QueueLabelPolicy policy = Queue.QueueLabelPolicy.AND;
    Expression queueLabelExpression = lb.getEffectiveLabelExpr("good && big");
    Expression appLabelExpression = lb.getEffectiveLabelExpr("badlabel");
    
    Expression finalExpr = lb.constructAppLabel(policy,
        appLabelExpression,
        queueLabelExpression);
    
    assertEquals("((good&&big)&&badlabel)", finalExpr.toString());

    try {
      lb.isNodeApplicableForApp("perfnode204.qa.lab", finalExpr);
      fail("Evaluation should fail for: " + finalExpr.toString());
    } catch (IOException e) {
      // show go here
    }
   }
}
