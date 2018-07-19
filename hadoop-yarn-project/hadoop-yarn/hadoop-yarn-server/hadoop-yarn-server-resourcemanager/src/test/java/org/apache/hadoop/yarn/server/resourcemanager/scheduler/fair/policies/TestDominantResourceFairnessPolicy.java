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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FakeSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

/**
 * comparator.compare(sched1, sched2) < 0 means that sched1 should get a
 * container before sched2
 */
public class TestDominantResourceFairnessPolicy {

  private Comparator<Schedulable> createComparator(int clusterMem,
                                                   int clusterCpu) {
    DominantResourceFairnessPolicy policy = new DominantResourceFairnessPolicy();
    policy.initialize(BuilderUtils.newResource(clusterMem, clusterCpu));
    return policy.getComparator();
  }

  private Comparator<Schedulable> createComparator(int clusterMem,
                                                   int clusterCpu, int clusterDisk) {
    DominantResourceFairnessPolicy policy = new DominantResourceFairnessPolicy();
    policy.initialize(BuilderUtils.newResource(clusterMem, clusterCpu, clusterDisk));
    return policy.getComparator();
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage) {
    return createSchedulable(memUsage, cpuUsage, ResourceWeights.NEUTRAL, 0, 0);
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      int minMemShare, int minCpuShare) {
    return createSchedulable(memUsage, cpuUsage, ResourceWeights.NEUTRAL,
        minMemShare, minCpuShare);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage, double diskUsage,
                                        int minMemShare, int minCpuShare, double minDiskUsage) {
    return createSchedulable(memUsage, cpuUsage, diskUsage, ResourceWeights.NEUTRAL,
            minMemShare, minCpuShare, minDiskUsage);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      ResourceWeights weights) {
    return createSchedulable(memUsage, cpuUsage, weights, 0, 0);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage, double diskUsage) {
    return createSchedulable(memUsage, cpuUsage, diskUsage, ResourceWeights.NEUTRAL, 0, 0, 0);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage, double diskUsage,
                                        ResourceWeights weights) {
    return createSchedulable(memUsage, cpuUsage, diskUsage, weights, 0, 0, 0);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage, double diskUsage,
                                        ResourceWeights weights, int minMemShare, int minCpuShare, double minDiskUsage) {
    Resource usage = BuilderUtils.newResource(memUsage, cpuUsage, diskUsage);
    Resource minShare = BuilderUtils.newResource(minMemShare, minCpuShare, minDiskUsage);
    return new FakeSchedulable(minShare,
            Resources.createResource(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
            weights, Resources.none(), usage, 0L);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      ResourceWeights weights, int minMemShare, int minCpuShare) {
    Resource usage = BuilderUtils.newResource(memUsage, cpuUsage);
    Resource minShare = BuilderUtils.newResource(minMemShare, minCpuShare);
    return new FakeSchedulable(minShare,
        Resources.createResource(Integer.MAX_VALUE, Integer.MAX_VALUE),
        weights, Resources.none(), usage, 0l);
  }
  
  @Test
  public void testSameDominantResource() {
    assertTrue(createComparator(8000, 4).compare(
        createSchedulable(1000, 1),
        createSchedulable(2000, 1)) < 0);
  }

  @Test
  public void testDifferentDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(4000, 3),
        createSchedulable(2000, 5)) < 0);
  }

  @Test
  public void testOneIsNeedy() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(2000, 5, 0, 6),
        createSchedulable(4000, 3, 0, 0)) < 0);
  }

  @Test
  public void testBothAreNeedy() {
    assertTrue(createComparator(8000, 100).compare(
        // dominant share is 2000/8000
        createSchedulable(2000, 5),
        // dominant share is 4000/8000
        createSchedulable(4000, 3)) < 0);
    assertTrue(createComparator(8000, 100).compare(
        // dominant min share is 2/3
        createSchedulable(2000, 5, 3000, 6),
        // dominant min share is 4/5
        createSchedulable(4000, 3, 5000, 4)) < 0);
  }

  @Test
  public void testEvenWeightsSameDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, new ResourceWeights(2.0f)),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, new ResourceWeights(2.0f)),
        createSchedulable(1000, 2)) < 0);
  }

  @Test
  public void testEvenWeightsDifferentDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, new ResourceWeights(2.0f)),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, new ResourceWeights(2.0f)),
        createSchedulable(1000, 2)) < 0);
  }
  
  @Test
  public void testUnevenWeightsSameDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, new ResourceWeights(2.0f, 1.0f)),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, new ResourceWeights(1.0f, 2.0f)),
        createSchedulable(1000, 2)) < 0);
  }

  @Test
  public void testUnevenWeightsDifferentDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, new ResourceWeights(1.0f, 2.0f)),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, new ResourceWeights(2.0f, 1.0f)),
        createSchedulable(1000, 2)) < 0);
  }

  @Test
  public void testCalculateShares() {
    Resource used = Resources.createResource(10, 5);
    Resource capacity = Resources.createResource(100, 10);
    ResourceType[] resourceOrder = new ResourceType[3];
    ResourceWeights shares = new ResourceWeights();
    DominantResourceFairnessPolicy.DominantResourceFairnessComparator comparator =
        new DominantResourceFairnessPolicy.DominantResourceFairnessComparator();
    comparator.calculateShares(used, capacity, shares, resourceOrder,
        ResourceWeights.NEUTRAL);

    assertEquals(.1, shares.getWeight(ResourceType.MEMORY), .00001);
    assertEquals(.5, shares.getWeight(ResourceType.CPU), .00001);
    assertEquals(ResourceType.CPU, resourceOrder[0]);
    assertEquals(ResourceType.MEMORY, resourceOrder[1]);
  }

  @Test
  public void testSameDominantResourceIncludeDisks() {
    assertTrue(createComparator(8000, 4, 1).compare(
            createSchedulable(1000, 1, 0.3),
            createSchedulable(2000, 1, 0.5)) < 0);
  }

  @Test
  public void testOneIsNeedyIncludeDisks() {
    ResourceWeights resourceWeights = new ResourceWeights(1.0f, 1.0f);
    resourceWeights.setWeight(ResourceType.DISK, 2.0f);

    assertTrue(createComparator(8000, 4, 1).compare(
            createSchedulable(2000, 1, 0, resourceWeights),
            createSchedulable(1000, 1, 1, resourceWeights )) < 0);
  }

  @Test
  public void testBothAreNeedyIncludeDisks() {
    assertTrue(createComparator(8000, 100, 2).compare(
            // dominant share is 2000/8000
            createSchedulable(2000, 5, 0.7),
            // dominant share is 4000/8000
            createSchedulable(4000, 3, 0.3)) < 0);
    assertTrue(createComparator(8000, 100, 2).compare(
            // dominant min share is 2/3
            createSchedulable(2000, 5, 0.3, 3000, 6, 0.5),
            // dominant min share is 4/5
            createSchedulable(4000, 3, 0.4, 5000, 4, 0.6)) < 0);
  }

  @Test
  public void testUnevenWeightsSameDominantResourceIncludeDisks() {
    ResourceWeights resourceWeights = new ResourceWeights(2.0f, 1.0f);
    resourceWeights.setWeight(ResourceType.DISK, 2.0f);

    assertTrue(createComparator(8000, 8, 3).compare(
            createSchedulable(3000, 1, 2, resourceWeights),
            createSchedulable(2000, 1, 2)) < 0);
    assertTrue(createComparator(8000, 8, 3).compare(
            createSchedulable(1000, 3, 2, resourceWeights),
            createSchedulable(1000, 2, 2)) < 0);
    assertTrue(createComparator(8000, 8, 3).compare(
            createSchedulable(1000, 3, 3, resourceWeights),
            createSchedulable(1000, 3, 2)) < 0);
  }

  @Test
  public void testCalculateSharesIncludeDisks() {
    Resource used = Resources.createResource(10, 5, 2);
    Resource capacity = Resources.createResource(100, 10, 8);
    ResourceType[] resourceOrder = new ResourceType[3];
    ResourceWeights shares = new ResourceWeights();
    DominantResourceFairnessPolicy.DominantResourceFairnessComparator comparator =
            new DominantResourceFairnessPolicy.DominantResourceFairnessComparator();
    comparator.calculateShares(used, capacity, shares, resourceOrder,
            ResourceWeights.NEUTRAL);

    assertEquals(.1, shares.getWeight(ResourceType.MEMORY), .00001);
    assertEquals(.5, shares.getWeight(ResourceType.CPU), .00001);
    assertEquals(.25, shares.getWeight(ResourceType.DISK), .00001);
    assertEquals(ResourceType.CPU, resourceOrder[0]);
    assertEquals(ResourceType.DISK, resourceOrder[1]);
    assertEquals(ResourceType.MEMORY, resourceOrder[2]);
  }

  @Test
  public void testModWhileSorting(){
    final List<FakeSchedulable> schedulableList = new ArrayList<>();
    for (int i=0; i<10000; i++) {
      schedulableList.add(
          (FakeSchedulable)createSchedulable((i%10)*100, (i%3)*2));
    }
    Comparator DRFComparator = createComparator(100000, 50000);

    // To simulate unallocated resource changes
    Thread modThread = modificationThread(schedulableList);
    modThread.start();

    // This should fail: make sure that we do test correctly
    // TimSort which is used does not handle the concurrent modification of
    // objects it is sorting.
    try {
      Collections.sort(schedulableList, DRFComparator);
      fail("Sorting should have failed and did not");
    } catch (IllegalArgumentException iae) {
      assertEquals(iae.getMessage(), "Comparison method violates its general contract!");
    }
    try {
      modThread.join();
    } catch (InterruptedException ie) {
      fail("ModThread join failed: " + ie.getMessage());
    }

    // clean up and try again using TreeSet which should work
    schedulableList.clear();
    for (int i=0; i<10000; i++) {
      schedulableList.add(
          (FakeSchedulable)createSchedulable((i%10)*100, (i%3)*2));
    }
    TreeSet<Schedulable> sortedSchedulable = new TreeSet<>(DRFComparator);
    modThread = modificationThread(schedulableList);
    modThread.start();
    sortedSchedulable.addAll(schedulableList);
    try {
      modThread.join();
    } catch (InterruptedException ie) {
      fail("ModThread join failed: " + ie.getMessage());
    }
  }

  /**
   * Thread to simulate concurrent schedulable changes while sorting
   */
  private Thread modificationThread(final List<FakeSchedulable> schedulableList) {
    Thread modThread  = new Thread() {
      @Override
      public void run() {
        try {
          // This sleep is needed to make sure the sort has started before the
          // modifications start and finish
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          fail("Modification thread interrupted while asleep " +
              ie.getMessage());
        }
        Resource newUsage = Resources.createResource(0, 0);
        for (int j = 0; j < 1000; j++) {
          FakeSchedulable sched = schedulableList.get(j * 10);
          newUsage.setMemory(20000);
          newUsage.setVirtualCores(j % 10);
          sched.setResourceUsage(newUsage);
        }
      }
    };
    return modThread;
  }
}
