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
package org.apache.hadoop.yarn.sls.utils;

import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.lang.String.format;

public class SchedulerUtils {

  private static final Random RANDOM = new Random();
  private static final List<String> labelExpressions = Arrays.asList("||", "&amp;&amp;");

  public static String generateFairSchedulerXml(int maxRunningJobs, int userMaxAppsDefault, int maxRunningAppsPerQueue,
                                                int numQueues, String schedulingPolicy, int minSharePreemptionTimeout,
                                                Set<String> labels) {
    StringBuilder fs = new StringBuilder();
    fs.append("<?xml version=\"1.0\"?>\n");

    fs.append("<allocations>\n");
    fs.append("  <user name=\"default\">\n");
    fs.append(format("    <maxRunningJobs>%d</maxRunningJobs>\n", maxRunningJobs));
    fs.append("  </user>\n");
    fs.append(format("  <userMaxAppsDefault>%d</userMaxAppsDefault>\n", userMaxAppsDefault));

    double weight = 1.0 / numQueues;
    for (int i = 1; i <= numQueues; i++) {
      fs.append(format("<queue name=\"sls_queue_%d\">\n", i));
      fs.append(format("<maxRunningApps>%d</maxRunningApps>\n", maxRunningAppsPerQueue));
      fs.append(format("    <schedulingPolicy>%s</schedulingPolicy>\n", schedulingPolicy));
      fs.append(format("    <weight>%f</weight>\n", weight));
      fs.append(format("    <minSharePreemptionTimeout>%d</minSharePreemptionTimeout>\n", minSharePreemptionTimeout));
      if (labels != null) {
        fs.append(format("    <label>%s</label>\n",
                StringUtils.join(labelExpressions.get(RANDOM.nextInt(labelExpressions.size())),
                        getRandomLabels(labels))));
      }
      fs.append("    <weight>1.0</weight>");
      fs.append("    <aclSubmitApps>*</aclSubmitApps>");
      fs.append("  </queue>\n");
    }
    fs.append("</allocations>");

    return fs.toString();
  }

  public static String generateCapacitySchedulerXml(int numQueues, double maximumCapacityPerQueue,
                                                    int maxRunningAppsPerQueue,
                                                    Set<String> labels) {
    StringBuilder cs = new StringBuilder();
    cs.append("<?xml version=\"1.0\"?>\n");
    cs.append("<configuration>\n");

    double queueCapacity = 100.0 / numQueues;
    StringBuilder queues = new StringBuilder();
    for (int queueNumber = 1; queueNumber <= numQueues; queueNumber++) {
      cs.append(generateProperty(
              String.format("yarn.scheduler.capacity.root.sls_queue_%d.capacity", queueNumber),
              String.valueOf(queueCapacity)));

      cs.append(generateProperty(
              String.format("yarn.scheduler.capacity.root.sls_queue_%d.maximum-capacity", queueNumber),
              String.valueOf(maximumCapacityPerQueue)));

      if (labels != null) {
        cs.append(generateProperty(
                String.format("yarn.scheduler.capacity.root.sls_queue_%d.label", queueNumber),
                StringUtils.join(labelExpressions.get(RANDOM.nextInt(labelExpressions.size())),
                        getRandomLabels(labels))));
      }

      queues.append(format("sls_queue_%d", queueNumber));
      if (queueNumber != numQueues) queues.append(",");
    }

    cs.append(generateProperty(
            "yarn.scheduler.capacity.root.queues",
            queues.toString()));

    // default resource-calculator is not suitable for our purposes, because it uses memory only
    cs.append(generateProperty(
            "yarn.scheduler.capacity.resource-calculator",
            "org.apache.hadoop.yarn.util.resource.DiskBasedDominantResourceCalculator"));

    cs.append("</configuration>");

    return cs.toString();
  }

  public static void setScheduler(Path yarnSitePath, String scheduler) throws IOException {
    String fairScheduler = "fair.FairScheduler";
    String capacityScheduler = "capacity.CapacityScheduler";
    String forReplace = scheduler.equals(fairScheduler) ? capacityScheduler : fairScheduler;

    String oldYarnSiteXml = new String(Files.readAllBytes(yarnSitePath), StandardCharsets.UTF_8);
    String newYarnSiteXml = oldYarnSiteXml.replace(forReplace, scheduler);
    Files.write(yarnSitePath, newYarnSiteXml.getBytes());
  }

  public static String generateNodeLabels(int numNodes, Set<String> labels) {
    StringBuilder nl = new StringBuilder();
    for (int i = 1; i <= numNodes; i++) {
      nl.append(i)
              .append(" ")
              .append(StringUtils.join(", ", getRandomLabels(labels)))
              .append("\n");
    }
    return nl.toString();
  }

  public static String generateProperty(String name, String value) {
    return "<property>\n" +
            "    <name>" + name + "</name>" +
            "    <value>" + value + "</value>" +
            "</property>\n";
  }

  private static Set<String> getRandomLabels(Set<String> labels) {
    List<String> labelsList = new ArrayList<>(labels);
    Collections.shuffle(labelsList);
    int toIndex = RANDOM.nextInt(labels.size());
    return new HashSet<>(labelsList.subList(0, toIndex == 0 ? toIndex + 1 : toIndex));
  }
}