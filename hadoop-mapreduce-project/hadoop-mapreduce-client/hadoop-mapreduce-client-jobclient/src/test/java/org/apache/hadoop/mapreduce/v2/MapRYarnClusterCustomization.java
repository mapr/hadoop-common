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

package org.apache.hadoop.mapreduce.v2;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniMapRFSCluster;

/**
 * MapR specific customizations to be applied while creating a mini MR cluster.
 * It overrides some configurations specified in
 * <code>MR_CONFIG_OVERRIDE_FILE</code> and creates a mini DFS cluster based on
 * MapR.
 */
public class MapRYarnClusterCustomization implements MiniMRYarnClusterCustomization {

  private static final Log LOG = LogFactory.getLog(MiniMRYarnCluster.class);

  private static final String MR_CONFIG_OVERRIDE_FILE = "mr_config.properties";

  private MiniMapRFSCluster dfsCluster;

  /**
   * {@inheritDoc}
   * @see MiniMRYarnClusterCustomization#overrideConfigFromFile(Configuration)
   */
  public void overrideConfigFromFile(Configuration conf) {
    MiniDFSCluster runningInstance = MiniDFSCluster.getRunningInstance();
    if (runningInstance != null && !(runningInstance instanceof MiniMapRFSCluster)) {
      LOG.info("Skipping MapR config oerride as different cluster type is"
          + " running: " + runningInstance.getClass().getName());

      return;
    }

    Properties props = new Properties();
    try {
      props.load(this.getClass().getClassLoader().getResourceAsStream(MR_CONFIG_OVERRIDE_FILE));

      LOG.info("Overriding conf with MapR settings");
      for (Map.Entry entry : props.entrySet()) {
        conf.set((String) entry.getKey(), (String) entry.getValue());
      }
    } catch (IOException e) {
      throw new RuntimeException("Cannot open MapR settings file: " + MR_CONFIG_OVERRIDE_FILE);
    }
  }

  /**
   * {@inheritDoc}
   * @see MiniMRYarnClusterCustomization#setupServices(Configuration,int)
   */
  public void setupServices(Configuration conf, int numOfNMs) {
    LOG.info("Starting MapR services");

    MiniDFSCluster runningInstance = MiniDFSCluster.getRunningInstance();
    if (runningInstance != null) {
      if (runningInstance instanceof MiniMapRFSCluster) {
        LOG.info("MiniMapRFSCluster already running. Will reuse that.");
      } else {
        LOG.info("Skipping MapRFS cluster creation as different cluster type is"
            + " running: " + runningInstance.getClass().getName());
      }
    } else {
      try {
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(numOfNMs)
          .build(MiniMapRFSCluster.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * {@inheritDoc}
   * @see MiniMRYarnClusterCustomization#teardownServices()
   */
  public void teardownServices() {
    LOG.info("Shutting down MapR services");

    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

}
