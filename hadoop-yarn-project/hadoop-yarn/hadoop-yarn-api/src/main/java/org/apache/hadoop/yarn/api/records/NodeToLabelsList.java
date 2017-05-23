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

package org.apache.hadoop.yarn.api.records;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>NodeToLabelsList</code> contains a regular expression that matches
 * a node or set of nodes and a list of labels associated with them.
 * </p>
 */
@Public
@Unstable
public abstract class NodeToLabelsList {
  @Private
  @Unstable
  public static NodeToLabelsList newInstance(String node, List<String> nodeLabel) {
    NodeToLabelsList nodeToLabelsList = Records.newRecord(NodeToLabelsList.class);
    nodeToLabelsList.setNode(node);
    nodeToLabelsList.setNodeLabel(nodeLabel);
    return nodeToLabelsList;
  }

  @Public
  @Unstable
  public abstract String getNode();

  @Public
  @Unstable
  public abstract void setNode(String node);

  @Public
  @Unstable
  public abstract List<String> getNodeLabel();

  @Public
  @Unstable
  public abstract void setNodeLabel(List<String> nodeLabel);

}
