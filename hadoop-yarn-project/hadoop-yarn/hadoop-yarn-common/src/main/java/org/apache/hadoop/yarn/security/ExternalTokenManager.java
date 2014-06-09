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
package org.apache.hadoop.yarn.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

/**
 * Interface to manage distribution/vendor specific tokens that are needed
 * to secure the communication. These are tokens that are different from the
 * default job tokens and container tokens.
 *
 * @see{ExternalTokenLocalizer}
 */
public interface ExternalTokenManager {
  /**
   * Uploads token to DFS for the given application.
   * This is invoked when client submits the application. So it will
   * be executed using the client privileges.
   *
   * @param appId application id
   */
  void uploadTokenToDistributedCache(ApplicationId appId);

  /**
   * Generates a token and stores it in DFS. This is invoked on the
   * resource manager node before the application is launched. It will be
   * executed using the privileges of the user the resource manager runs as.
   * When the container starts on a node, the token will be localized
   * by @link{ResourceLocalizationService}.
   */
  void generateToken(ApplicationSubmissionContext appCtx, String username,
      Configuration conf);

  /**
   * Deletes the token from DFS. This is invoked on the resource manager node
   * after the application completes.
   */
  void removeToken(ApplicationId appId, Configuration conf);
}
