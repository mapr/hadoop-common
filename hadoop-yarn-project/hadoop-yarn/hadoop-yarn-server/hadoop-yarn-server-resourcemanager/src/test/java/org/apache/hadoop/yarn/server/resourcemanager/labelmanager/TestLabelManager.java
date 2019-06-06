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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue.QueueLabelPolicy;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLabelManager {

  private static Configuration conf;
  private static FileSystem fs;
  private static LabelManagementService lbS;

  private final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  private final static String LABEL_FILE = TEST_DIR + "/labelFile";
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.set(LabelManager.NODE_LABELS_FILE, LABEL_FILE);
    conf.setLong(LabelManager.NODE_LABELS_MONITOR_INTERVAL, 5*1000);
    fs = FileSystem.getLocal(conf);

    PrintWriter out = new PrintWriter(new FileWriter(LABEL_FILE));
    out.println("perfnode200.* big, \"Production Machines\"");
    out.println("perfnode203.* big, 'Development Machines'");
    out.println("perfnode15* good");
    out.println("perfnode1* right, good, fantastic");
    out.println("perfnode201* slow");
    out.println("perfnode204* good, big");
    out.println("node-.+lab     Fast");
    out.println("node-2*      Slow");
    out.close();
    
    lbS = new LabelManagementService();
    lbS.init(conf);
    lbS.start();
    assertTrue(lbS.getServiceState() == Service.STATE.STARTED);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    lbS.stop();
    assertFalse(lbS.getServiceState() != Service.STATE.STOPPED);
    fs.delete(new Path(LABEL_FILE), false);
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(LabelManager.NODE_LABELS_FILE, LABEL_FILE);
    conf.setLong(LabelManager.NODE_LABELS_MONITOR_INTERVAL, 5*1000);

    PrintWriter out = new PrintWriter(new FileWriter(LABEL_FILE));
    out.println("perfnode200.* big, \"Production Machines\"");
    out.println("perfnode203.* big, 'Development Machines'");
    out.println("perfnode15* good");
    out.println("perfnode1* right, good, fantastic");
    out.println("perfnode201* slow");
    out.println("perfnode204* good, big");
    out.println("node-.+lab     Fast");
    out.println("node-2*      Slow");
    out.close();
    
    LabelManager lb = LabelManager.getInstance();
    lb.refreshLabels(conf);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(LABEL_FILE), false);
  }

  @Test (timeout=10000)
  public void testLabelManager() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue(LABEL_FILE.equalsIgnoreCase(labelFile.toString()));
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
    assertTrue(LABEL_FILE.equalsIgnoreCase(labelFile.toString()));
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
    assertTrue(LABEL_FILE.equalsIgnoreCase(labelFile.toString()));
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
    assertTrue(LABEL_FILE.equalsIgnoreCase(labelFile.toString()));
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
  public void testLabelsUpdateAfterMonitorInterval() throws Exception {
    LabelManager lb = LabelManager.getInstance();
    Path labelFile = lb.getLabelFile();
    assertNotNull(labelFile);
    assertTrue(LABEL_FILE.equalsIgnoreCase(labelFile.toString()));
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

    fs.delete(new Path(LABEL_FILE), false);
    
    FSDataOutputStream fsout = fs.create(new Path(LABEL_FILE));
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
  public void testGetEffectiveLabelExpr() throws IOException{
    LabelManager lb = LabelManager.getInstance();

    Expression labelExpr = lb.getEffectiveLabelExpr("High Memory");
    assertEquals("(High Memory)", labelExpr.toString());
    labelExpr = lb.getEffectiveLabelExpr("'High Memory'");
    assertEquals("(High Memory)", labelExpr.toString());
    labelExpr = lb.getEffectiveLabelExpr("\"High Memory\"");
    assertEquals("(High Memory)", labelExpr.toString());
    labelExpr = lb.getEffectiveLabelExpr("Slow && Fast");
    assertEquals("(Slow&&Fast)", labelExpr.toString());
    labelExpr = lb.getEffectiveLabelExpr("Slow A && Slow B && 'Slow C'");
    assertEquals("((Slow A&&Slow B)&&Slow C)", labelExpr.toString());

    labelExpr = lb.getEffectiveLabelExpr("*");
    assertNull(labelExpr);
    labelExpr = lb.getEffectiveLabelExpr("all");
    assertNull(labelExpr);
    labelExpr = lb.getEffectiveLabelExpr(null);
    assertNull(labelExpr);
  }

  @Test
  public void testLabelsRefresh() throws IOException {
    LabelManager lb = LabelManager.getInstance();

    fs.delete(new Path(LABEL_FILE), false);

    PrintWriter out = new PrintWriter(new FileWriter(LABEL_FILE));
    out.println("node1  Plain");
    out.println("node2  Large");
    out.close();

    lb.refreshLabels(conf);

    Set<String> node1Labels = lb.getLabelsForNode("node1");
    Set<String> node2Labels = lb.getLabelsForNode("node2");
    assertEquals(node1Labels.size(), 1);
    assertTrue(node1Labels.contains("Plain"));
    assertEquals(node2Labels.size(), 1);
    assertTrue(node2Labels.contains("Large"));

    // Refreshing when the labels file was changed
    fs.delete(new Path(LABEL_FILE), false);

    out = new PrintWriter(new FileWriter(LABEL_FILE));
    out.println("node1  Dev");
    out.println("node2  Prod");
    out.close();

    lb.refreshLabels(conf);

    node1Labels = lb.getLabelsForNode("node1");
    node2Labels = lb.getLabelsForNode("node2");
    assertEquals(node1Labels.size(), 1);
    assertTrue(node1Labels.contains("Dev"));
    assertEquals(node2Labels.size(), 1);
    assertTrue(node2Labels.contains("Prod"));
    
    // Refreshing when path to labels file is null
    lb.refreshLabels(new Configuration());
    Set<Expression> labels = lb.getLabels();
    assertTrue(labels.isEmpty());

    // Refreshing when labels file doesn't exist
    conf.set(LabelManager.NODE_LABELS_FILE, "/notexist");
    lb.refreshLabels(conf);
    labels = lb.getLabels();
    assertTrue(labels.isEmpty());
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
  
  //@Test - Use only when testing performance
  public void testPerformance() throws Exception {
    fs.delete(new Path(LABEL_FILE), false);
    
    FSDataOutputStream fsout = fs.create(new Path(LABEL_FILE));
    for ( int i = 0; i < 1000; i++ ) {
      String node = "node-2."+i+".lab";
      if ( i % 3  == 0 ) {
        fsout.writeBytes(node + "      Slow");
      } else if ( i % 5 == 0 ) {
        fsout.writeBytes(node + "      good");
      } else if ( i % 7 == 0 ) {
        fsout.writeBytes(node + "      Fast");
      } else {
        fsout.writeBytes(node + "      Slow, good, small");
      }
      fsout.writeBytes("\n");
    }
    fsout.close();

    // make sure file refreshed
    Thread.sleep(6*1000l);

    LabelManager lb = LabelManager.getInstance();
    
    Expression finalExp = 
        lb.constructAppLabel(QueueLabelPolicy.OR, 
            new Expression("Slow || Fast"), new Expression("Slow"));
    
    Expression appExp = new Expression("Slow || Fast");
    Expression qExp = new Expression("Slow");
    int i = 0;
    long startTime = System.currentTimeMillis();
    while (i < 1000000) {
      String resourceName = "node-2."+i%1000+".lab";
      i++;
      lb.isNodeApplicableForApp(resourceName, finalExp);
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println("Time taken: " + (endTime - startTime) + " ms");

    i = 0;
    startTime = System.currentTimeMillis();
    while (i < 1000000) {
      String resourceName = "node-2."+i%1000+".lab";
      i++;

      lb.isNodeApplicableForApp(resourceName, finalExp);
    }
    endTime = System.currentTimeMillis();
    
    System.out.println("Time taken2: " + (endTime - startTime) + " ms");

    i = 0;
    startTime = System.currentTimeMillis();
    while (i < 3000000) {
      String resourceName = "/defaultrack";
      i++;

      lb.isNodeApplicableForApp(resourceName, finalExp);
    }
    endTime = System.currentTimeMillis();
    
    System.out.println("Time taken3: " + (endTime - startTime) + " ms");
  }
}
