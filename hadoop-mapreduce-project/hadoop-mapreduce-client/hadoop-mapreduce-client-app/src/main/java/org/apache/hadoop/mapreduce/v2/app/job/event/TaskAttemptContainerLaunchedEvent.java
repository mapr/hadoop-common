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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptContainerLaunchedEvent extends TaskAttemptEvent {
  
  static final Log LOG = LogFactory.getLog(TaskAttemptContainerLaunchedEvent.class);
  
  private int shufflePort;
  // Inefficient - does it for every container launch
  private Map<String, ByteBuffer> servicesMetaInfo = 
      Collections.synchronizedMap(new HashMap<String,ByteBuffer>());
  /**
   * Create a new TaskAttemptEvent.
   * @param id the id of the task attempt
   * @param shufflePort the port that shuffle is listening on.
   */
  public TaskAttemptContainerLaunchedEvent(TaskAttemptId id, int shufflePort) {
    super(id, TaskAttemptEventType.TA_CONTAINER_LAUNCHED);
    this.shufflePort = shufflePort;
  }

  public TaskAttemptContainerLaunchedEvent(TaskAttemptId id, 
      Map<String, ByteBuffer> servicesMetaInfo) {
    super(id, TaskAttemptEventType.TA_CONTAINER_LAUNCHED);
    // To support backwards compatibility with shufflePort
    this.setServicesMetaInfo(servicesMetaInfo);
    ByteBuffer portInfo =
        servicesMetaInfo.get(
            ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
    int port = -1;
    if(portInfo != null) {
      try {
        port = ShuffleHandler.deserializeMetaData(portInfo);
      } catch (IOException e) {
        if ( LOG.isDebugEnabled()) {
          LOG.info(
            "Shuffle port is not found - possibly different shuffle service is used", e);
        }
      }
    }
    LOG.info("Shuffle port returned by ContainerManager for "
        + id + " : " + port);
    this.shufflePort = port;

    if(port < 0) {
      LOG.info("Shuffle port is not found - possibly different shuffle service is used");
    }
  }
  
  /**
   * Get the port that the shuffle handler is listening on. This is only
   * valid if the type of the event is TA_CONTAINER_LAUNCHED
   * @return the port the shuffle handler is listening on.
   */
  public int getShufflePort() {
    return shufflePort;
  }

  public Map<String, ByteBuffer> getServicesMetaInfo() {
    Map<String, ByteBuffer> metaClone = new HashMap<String, ByteBuffer>(
        servicesMetaInfo.size());
    synchronized (servicesMetaInfo) {
      for (Entry<String, ByteBuffer> entry : servicesMetaInfo.entrySet()) {
        metaClone.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
    return metaClone;

  }

  private void setServicesMetaInfo(Map<String, ByteBuffer> serviceMetaData) {
    synchronized (servicesMetaInfo) {
      for (Entry<String, ByteBuffer> entry : serviceMetaData.entrySet()) {
        servicesMetaInfo.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
  }
}
