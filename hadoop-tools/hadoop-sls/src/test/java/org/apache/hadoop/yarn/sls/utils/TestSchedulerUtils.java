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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestSchedulerUtils {

  @Test
  public void testGenerateFairSchedulerXml() throws IOException {
    String fs = SchedulerUtils.generateFairSchedulerXml(1000, 1000, 50,
            40, "fair", 2,
            null);

    Assert.assertTrue(fs.length() > 0);
    Assert.assertTrue(fs.contains("sls_queue_40"));
    Assert.assertTrue(fs.contains("fair"));
  }

  @Test
  public void testGenerateFairSchedulerXmlWithLabels() {
    Set<String> labels = new HashSet<>();
    labels.add("dev");
    labels.add("test");
    labels.add("prod");

    String fs = SchedulerUtils.generateFairSchedulerXml(1000, 1000, 50,
            40, "fair", 2,
            labels);

    Assert.assertTrue(fs.contains("dev"));
    Assert.assertTrue(fs.contains("prod"));
    Assert.assertTrue(fs.contains("test"));
  }

  @Test
  public void testGenerateCapacitySchedulerXml() throws IOException {
    String cs = SchedulerUtils.generateCapacitySchedulerXml(40, 75, 1000, null);
    Assert.assertTrue(cs.length() > 0);
    Assert.assertTrue(cs.contains("sls_queue_40"));
  }

  @Test
  public void testGenerateCapacitySchedulerXmlWithLabels() {
    Set<String> labels = new HashSet<>();
    labels.add("dev");
    labels.add("test");
    labels.add("prod");

    String fs = SchedulerUtils.generateCapacitySchedulerXml(40, 75, 50, labels);

    Assert.assertTrue(fs.contains("dev"));
    Assert.assertTrue(fs.contains("prod"));
    Assert.assertTrue(fs.contains("test"));
  }

  @Test
  public void testGenerateNodeLabels() {
    Set<String> nodes = new HashSet<>();
    nodes.add("1");
    nodes.add("2");
    nodes.add("3");

    Set<String> labels = new HashSet<>();
    labels.add("dev");
    labels.add("test");
    labels.add("prod");

    String nodeLabels = SchedulerUtils.generateNodeLabels(3, labels);

    Assert.assertTrue(nodeLabels.length() > 0);
    Assert.assertTrue(nodeLabels.contains("1"));
    Assert.assertTrue(nodeLabels.contains("2"));
    Assert.assertTrue(nodeLabels.contains("3"));

  }
}