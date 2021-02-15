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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testing applications being retired from RM with fair scheduler.
 *
 */
public class TestAppManagerWithFairScheduler extends AppManagerTestBase {

  public final static String TEST_DIR =
          new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  private final static String ALLOC_FILE = new File(TEST_DIR,
          TestAppManagerWithFairScheduler.class.getName() + ".xml").getAbsolutePath();
  private static YarnConfiguration conf = new YarnConfiguration();

  @BeforeClass
  public static void setup() throws IOException {
    int queueMaxAllocation = 512;

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println(" <queue name=\"queueA\">");
    out.println("  <maxContainerAllocation>" + queueMaxAllocation
            + " mb 1 vcores 1 disks" + "</maxContainerAllocation>");
    out.println(" </queue>");
    out.println(" <queue name=\"queueB\">");
    out.println(" </queue>");
    out.println("</allocations>");
    out.close();

    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
            ResourceScheduler.class);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
  }

  @Test
  public void testQueueSubmitWithHighQueueContainerSize()
          throws YarnException {

    ApplicationId appId = MockApps.newAppID(1);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    Resource resource = Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext asContext =
            recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    asContext.setApplicationId(appId);
    asContext.setResource(resource);
    asContext.setPriority(Priority.newInstance(0));
    asContext.setAMContainerSpec(mockContainerLaunchContext(recordFactory));

    MockRM newMockRM = new MockRM(conf);
    RMContext newMockRMContext = newMockRM.getRMContext();
    ApplicationMasterService masterService = new ApplicationMasterService(
            newMockRMContext, newMockRMContext.getScheduler());

    TestRMAppManager newAppMonitor = new TestRMAppManager(newMockRMContext,
            new ClientToAMTokenSecretManagerInRM(), newMockRMContext.getScheduler(),
            masterService, new ApplicationACLsManager(conf), conf);

    try {
      asContext.setQueue("queueA");
      newAppMonitor.submitApplication(asContext, "test");
      Assert.fail("Test should fail on too high allocation!");
    } catch (InvalidResourceRequestException e) {
      // Should throw exception
    }

    // Should not throw exception
    asContext.setQueue("queueB");
    newAppMonitor.submitApplication(asContext, "test");
  }

  private static ContainerLaunchContext mockContainerLaunchContext(
          RecordFactory recordFactory) {
    ContainerLaunchContext amContainer = recordFactory.newRecordInstance(
            ContainerLaunchContext.class);
    amContainer
            .setApplicationACLs(new HashMap<ApplicationAccessType, String>());
    return amContainer;
  }
}