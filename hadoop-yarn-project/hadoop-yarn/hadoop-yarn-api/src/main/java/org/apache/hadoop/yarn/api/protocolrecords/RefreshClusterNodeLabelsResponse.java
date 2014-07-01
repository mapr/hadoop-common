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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to the client for a
 * request to refresh labels for nodes in the cluster
 * </p>
 * <p>
 * The response, includes:
 * <ul>
 * <li>A flag which indicates that the task of refreshing labels is completed or not.</li>
 * </ul>
 * </p>
 * 
 * @see ApplicationClientProtocol#refreshClusterNodeLabels(RefreshClusterNodeLabelsRequest)
 */
@Public
@Unstable
public abstract class RefreshClusterNodeLabelsResponse {
  @Public
  @Unstable
  public static RefreshClusterNodeLabelsResponse newInstance(boolean isRefreshCompleted) {
	  RefreshClusterNodeLabelsResponse response =
        Records.newRecord(RefreshClusterNodeLabelsResponse.class);
    response.setIsRefreshLabelsComplete(isRefreshCompleted);
    return response;
  }

  /**
   * Get the flag which indicates that the task of refreshing labels is completed or not.
   */
  @Public
  @Unstable
  public abstract boolean getIsRefreshLabelsComplete();

  /**
   * Set the flag which indicates that the task of refreshing labels is completed or not.
   */
  @Public
  @Unstable
  public abstract void setIsRefreshLabelsComplete(boolean isRefreshCompleted);

}
