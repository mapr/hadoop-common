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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizerContext;

/**
 * Interface to plug in a token localizer that can localize any distribution
 * specific tokens from DFS on to some node local directory. These are tokens
 * that are different from the default job tokens and container tokens.
 *
 * <p>
 * The localized tokens will then be copied over to the container specific
 * temp directory before localizing other resources and also before launching
 * the task.
 *
 * @see{ExternalTokenManager}
 */
public interface ExternalTokenLocalizer {
  /**
   * Localizes any distribution specific tokens. This method will be invoked with
   * the privileges of the user account used to run NodeManager service.
   *
   * @param context localizer context
   * @param conf YarnConfiguration instance
   * @param localDirsHandlerService
   */
  void run(LocalizerContext context, Configuration conf,
      LocalDirsHandlerService localDirsHandlerService);

  /**
   * Returns the path of the localized token for given application id.
   *
   * @param appIdStr application id
   * @param conf YarnConfiguration instance
   */
  Path getTokenPath(String appIdStr, Configuration conf);

  /**
   * Returns the environment variable name that needs to be set to point to the
   * external token path. This environment variable will be available to both
   * the localization process and the actual container process running the task.
   */
  String getTokenEnvVar();
}
