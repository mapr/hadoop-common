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
package org.apache.hadoop.yarn.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Helper class to facilitate task logs to be directly written to DFS.
 *
 * DFS logging is enabled by setting the property
 * {@link YarnConfiguration#ENABLE_DFS_LOGGING} to <code>true</code> in
 * yarn-site.xml. If enabled, then the configured handler class is instantiated
 * to provide the DFS specific behavior.
 */
public class TaskLogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskLogUtil.class);

  private static final YarnConfiguration DEFAULT_CONF = DefaultYarnConfiguration.get();
  private static final boolean enableDfsLogging;
  private static DFSLoggingHandler dfsLoggingHandler = null;

  static {
    enableDfsLogging = Boolean.parseBoolean(
        DEFAULT_CONF.get(YarnConfiguration.ENABLE_DFS_LOGGING, "false"));

    if (enableDfsLogging) {
      initializeHandler();
    }
  }

  private static void initializeHandler() {
    String handlerClass = DEFAULT_CONF.get(
        YarnConfiguration.DFS_LOGGING_HANDLER_CLASS);

    try {
      dfsLoggingHandler = (DFSLoggingHandler) (Class.forName(handlerClass)
          .getDeclaredConstructor().newInstance());

    } catch (Exception e) {
      LOG.error("Cannot load redirection class: " + handlerClass, e);
      throw new RuntimeException(e);
    }
  }

  public static DFSLoggingHandler getDFSLoggingHandler() {
    return dfsLoggingHandler;
  }

  /**
   * Determines if logs should be saved directly in DFS or written to local
   * file system. It uses the default <code>YarnConfiguration</code> to
   * determine it. To override for specific application, use the overloaded
   * method.
   *
   * @return true if DFS logging is enabled globally; false otherwise
   */
  public static boolean isDfsLoggingEnabled() {
    return enableDfsLogging;
  }

  /**
   * Determines if logs should be saved directly in DFS or written to local
   * file system using the given environment. In addition to the global
   * setting defined in YarnConfiguration, it uses the given
   * <code>env</code> to determine if DFS logging is supported by the
   * application.
   *
   * @param env environment settings for the application
   * @return true if DFS logging is enabled globally and also in the given
   * environment; false otherwise
   */
  public static boolean isDfsLoggingEnabled(Map<String, String> env) {
    String dfsLoggingSupported = env.get(
        YarnConfiguration.DFS_LOGGING_SUPPORTED);

    return enableDfsLogging && dfsLoggingSupported != null
      && Boolean.parseBoolean(dfsLoggingSupported);
  }

  /**
   * Returns the cached default YarnConfiguration object used to initialize
   * settings.
   */
  public static YarnConfiguration getConf() {
    return DEFAULT_CONF;
  }

  /**
   * Returns the Log4j appender to use depending on whether DFS logging is
   * enabled or not.
   */
  public static String getAppender() {
    return enableDfsLogging ? "CLA_DFS" : "CLA";
  }

  /**
   * Returns the Log4j rolling appender to use depending on whether DFS logging
   * is enabled or not.
   */
  public static String getRollingAppender() {
    return enableDfsLogging ? "CRLA_DFS" : "CRLA";
  }

  /**
   * Returns the property value from cached default YarnConfiguration object.
   *
   * @return value if property is found; otherwise <code>defaultValue</code>
   * is returned
   */
  public static String getPropertyValue(String property, String defaultValue) {
    return DEFAULT_CONF.get(property, defaultValue);
  }

  /**
   * Returns the property value from cached default YarnConfiguration object.
   *
   * @return value if property is found; <code>null</code> otherwise
   */
  public static String getPropertyValue(String property) {
    return DEFAULT_CONF.get(property);
  }
}
