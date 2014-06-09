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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Factory class to return an instance of @link{ExternalTokenManager}.
 */
public final class ExternalTokenManagerFactory {
  private static final Log LOG = LogFactory.getLog(ExternalTokenManagerFactory.class);

  private static ExternalTokenManager extTokenManager;

  static {
    YarnConfiguration conf = DefaultYarnConfiguration.get();

    // Get the configured external token manager class and create an instance
    Class<? extends ExternalTokenManager> clazz = conf.getClass(
        YarnConfiguration.YARN_EXT_TOKEN_MANAGER,
        null,
        ExternalTokenManager.class);

    if (clazz != null) {
      try {
        extTokenManager = clazz.newInstance();
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("Initialized external token manager class - "
            + clazz.getName());
      }
    }
  }

  public static ExternalTokenManager get() {
    return extTokenManager;
  }
}
