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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Factory class to return an instance of @link{ExternalTokenLocalizer}.
 */
public final class ExternalTokenLocalizerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalTokenLocalizerFactory.class);

  private static ExternalTokenLocalizer extTokenLocalizer;

  static {
    YarnConfiguration conf = new YarnConfiguration();

    // Get the configured external localizer class and create an instance
    Class<? extends ExternalTokenLocalizer> clazz = conf.getClass(
        YarnConfiguration.YARN_NODEMANAGER_EXT_TOKEN_LOCALIZER,
        null,
        ExternalTokenLocalizer.class);

    if (clazz != null) {
      try {
        extTokenLocalizer = clazz.getDeclaredConstructor()
          .newInstance();
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("Initialized external token localizer class - "
            + clazz.getName());
      }
    }
  }

  public static ExternalTokenLocalizer get() {
    return extTokenLocalizer;
  }
}
