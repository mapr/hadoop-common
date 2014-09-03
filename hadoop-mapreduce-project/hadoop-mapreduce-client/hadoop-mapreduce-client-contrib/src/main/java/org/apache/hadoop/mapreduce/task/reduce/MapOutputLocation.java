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

package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

public class MapOutputLocation {
  TaskAttemptID taskAttemptId;
  TaskID taskId;
  String ttHost;
  PathId shuffleRootFid;

  public MapOutputLocation(TaskAttemptID taskAttemptId,
    String ttHost, PathId pathId)
  {
    this.taskAttemptId = taskAttemptId;
    this.taskId = this.taskAttemptId.getTaskID();
    this.ttHost = ttHost;
    this.shuffleRootFid = pathId;
  }

  public TaskAttemptID getTaskAttemptId() {
    return taskAttemptId;
  }

  public TaskID getTaskId() {
    return taskId;
  }

  public String getHost() {
    return ttHost;
  }

  public PathId getShuffleRootFid() {
    return shuffleRootFid;
  }
}
