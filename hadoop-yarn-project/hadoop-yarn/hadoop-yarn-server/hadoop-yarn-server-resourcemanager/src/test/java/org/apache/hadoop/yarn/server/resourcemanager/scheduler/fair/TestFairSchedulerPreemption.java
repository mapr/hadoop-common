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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManagementService;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerPreemption.class.getName() + ".xml").getAbsolutePath();
  private final static String LABEL_FILE = TEST_DIR + "/labelFile";
  private final static String STATIC_HOST = "127.0.0.1";

  private ControlledClock clock;

  private static class StubbedFairScheduler extends FairScheduler {
    public int lastPreemptMemory = -1;

    @Override
    protected void preemptResources(Map<FSAppAttempt, Resource> toPreempt) {
      Resource totalResource = Resources.createResource(0);
      for (Resource resource : toPreempt.values()) {
        totalResource = Resources.add(totalResource, resource);
      }
      lastPreemptMemory = totalResource.getMemory();
    }

    public void resetLastPreemptResources() {
      lastPreemptMemory = -1;
    }
  }

  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, StubbedFairScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    clock = new ControlledClock();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  private void startResourceManager(float utilizationThreshold) {
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD,
        utilizationThreshold);
    resourceManager = new MockRM(conf);
    resourceManager.start();

    assertTrue(
        resourceManager.getResourceScheduler() instanceof StubbedFairScheduler);
    scheduler = (FairScheduler)resourceManager.getResourceScheduler();

    scheduler.setClock(clock);
    scheduler.updateInterval = 60 * 1000;
  }

  private void registerNodeAndSubmitApp(
      int memory, int vcores, double disks, int appContainers, int appMemory) {
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(memory, vcores, disks), 1, "node1");
    NetUtils.addStaticResolution(node1.getHostName(), STATIC_HOST);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    assertEquals("Incorrect amount of resources in the cluster",
        memory, scheduler.rootMetrics.getAvailableMB());
    assertEquals("Incorrect amount of resources in the cluster",
        vcores, scheduler.rootMetrics.getAvailableVirtualCores());
    assertEquals("Incorrect amount of resources in the cluster",
        disks, scheduler.rootMetrics.getAvailableDisks(), 0.001);

    createSchedulingRequest(appMemory, "queueA", "user1", appContainers);
    scheduler.update();
    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);
    }
    assertEquals("app1's request is not met",
        memory - appContainers * appMemory,
        scheduler.rootMetrics.getAvailableMB());
  }

  @Test
  public void testPreemptionWithFreeResources() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores,0disks</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores,0disks</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores,0disks</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    startResourceManager(0f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 1, 2, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManager(0.8f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 1, 3, 1024);

    // Verify submitting another request doesn't trigger preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should not have been called", -1,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();

    startResourceManager(0.7f);
    // Create node with 4GB memory and 4 vcores
    registerNodeAndSubmitApp(4 * 1024, 4, 1, 3, 1024);

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "queueB", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);
  }

  @Test
  public void testPreemptionThresholdWithLbs() throws Exception {
    conf.set(LabelManager.NODE_LABELS_FILE, LABEL_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION_THRESHOLD_BASED_ON_LABELS_ENABLED, true);
    
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("  <maxResources>0mb,0vcores,0disks</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"plain1\">");
    out.println("  <weight>1</weight>");
    out.println("  <label>Plain</label>");
    out.println("  <minResources>1024mb,0vcores,0disks</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"plain2\">");
    out.println("  <weight>1</weight>");
    out.println("  <label>Plain</label>");
    out.println("  <minResources>1024mb,0vcores,0disks</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"large\">");
    out.println("  <weight>1</weight>");
    out.println("  <label>Large</label>");
    out.println("  <minResources>1024mb,0vcores,0disks</minResources>");
    out.println("</queue>");
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    out = new PrintWriter(new FileWriter(LABEL_FILE));
    out.println("node1  Plain");
    out.println("node2  Large");
    out.close();
    
    lbS = new LabelManagementService();
    lbS.init(conf);
    lbS.start();
    LabelManager lb = LabelManager.getInstance();
    lb.refreshLabels(conf);

    startResourceManager(0.7f);

    // Create node with 4GB memory and 4 vcores, 1 disk
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(4 * 1024, 4, 1), 1, "node1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    NetUtils.addStaticResolution(node1.getHostName(), STATIC_HOST);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(1024, "plain1", "user1", 3);
    scheduler.update();

    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);
    }
    
    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "plain2", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should have been called", 1024,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    resourceManager.stop();
    
    startResourceManager(0.8f);
    
    node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(4 * 1024, 4, 1), 1, "node1");
    nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    NetUtils.addStaticResolution(node1.getHostName(), STATIC_HOST);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(1024, "plain1", "user1", 3);
    scheduler.update();

    for (int i = 0; i < 3; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);
    }

    // Verify submitting another request triggers preemption
    createSchedulingRequest(1024, "plain2", "user1", 1, 1);
    scheduler.update();
    clock.tickSec(6);

    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should not have been called", -1,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);
    
    // Now create second node with 10GB memory, 10 vcores and 1 disk
    RMNode node2 = MockNodes.newNodeInfo(
        1, Resources.createResource(10 * 1024, 10, 1), 1, "node2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NetUtils.addStaticResolution(node2.getHostName(), STATIC_HOST);
    scheduler.handle(nodeEvent2);

    // Verify submitting another request doesn't trigger preemption 
    // because we still not exceed utilization threshold for "Plain" label
    createSchedulingRequest(1024, "large", "user1", 10);
    scheduler.update();

    for (int i = 0; i < 10; i++) {
      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);
    }
    
    ((StubbedFairScheduler) scheduler).resetLastPreemptResources();
    scheduler.preemptTasksIfNecessary();
    assertEquals("preemptResources() should not have been called", -1,
        ((StubbedFairScheduler) scheduler).lastPreemptMemory);

    lbS.stop();
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(new Path(LABEL_FILE), false);
    lb.refreshLabels(conf);
  }
}
