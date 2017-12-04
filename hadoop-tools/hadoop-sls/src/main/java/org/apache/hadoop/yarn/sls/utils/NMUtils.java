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

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.util.Records;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Private
@Unstable
@SuppressWarnings("all")
public class NMUtils {

  private static SLSRunner slsRunner;
  private static ResourceManager rm;
  private static HashMap<NodeId, NMSimulator> nmMap;
  private static Set<RMNode> restarted = new HashSet<>();

  public static void stopNodes() throws IllegalAccessException {
    getFields();

    for (NMSimulator nmSimulator : nmMap.values()) {
      RMNode node = nmSimulator.getNode();
      if (Math.random() > 0.5) { // restart random number of NMs
        rm.getRMContext().getDispatcher().getEventHandler().handle(new RMNodeEvent(node.getNodeID(),
                RMNodeEventType.REBOOTING));
        restarted.add(node);
      }
    }
  }

  /**
   * restart NodeManagers by registering them again(old nodes rejoining) using RecourseManager
   */
  public static void restartNodes() {
    getFields();

    for (RMNode node : restarted) {
      RegisterNodeManagerRequest req =
              Records.newRecord(RegisterNodeManagerRequest.class);
      req.setNodeId(node.getNodeID());
      req.setResource(node.getTotalCapability());
      req.setHttpPort(80);
      try {
        rm.getResourceTrackerService().registerNodeManager(req);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void getFields() {
    try {
      slsRunner = (SLSRunner) FieldUtils.readStaticField(SLSRunner.class, "sls", true);
      rm = (ResourceManager) FieldUtils.readDeclaredField(slsRunner, "rm", true);
      nmMap = (HashMap<NodeId, NMSimulator>) FieldUtils.readField(slsRunner, "nmMap", true);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}