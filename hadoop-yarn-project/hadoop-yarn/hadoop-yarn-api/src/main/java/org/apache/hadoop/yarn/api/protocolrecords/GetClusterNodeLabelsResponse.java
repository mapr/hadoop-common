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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * labels for nodes in the cluster.
 * </p>
 * 
 * <p>
 * The response includes a list of {@link NodeToLabelsList} each of which has labels
 * for a single node.
 * </p>
 * 
 */
@Public
@Unstable
public abstract class GetClusterNodeLabelsResponse {

  @Public
  @Unstable
  public static GetClusterNodeLabelsResponse newInstance(
    List<NodeToLabelsList> clusterNodeLabels) {
    GetClusterNodeLabelsResponse response =
        Records.newRecord(GetClusterNodeLabelsResponse.class);
    response.setClusterNodeLabels(clusterNodeLabels);
    return response;
  }

  @Public
  @Unstable
  public abstract List<NodeToLabelsList> getClusterNodeLabels();

  @Public
  @Unstable
  public abstract void setClusterNodeLabels(List<NodeToLabelsList> clusterNodeLabels);

}
