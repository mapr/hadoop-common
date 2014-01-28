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

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.fs.PathId;

public interface TaskAttemptCompletionEvent {
  public abstract TaskAttemptId getAttemptId();
  public abstract TaskAttemptCompletionEventStatus getStatus();
  public abstract String getMapOutputServerAddress();
  public abstract int getAttemptRunTime();
  public abstract int getEventId();
  public abstract PathId getPathId();
  
  public abstract void setAttemptId(TaskAttemptId taskAttemptId);
  public abstract void setStatus(TaskAttemptCompletionEventStatus status);
  public abstract void setMapOutputServerAddress(String address);
  public abstract void setAttemptRunTime(int runTime);
  public abstract void setEventId(int eventId);
  public abstract void setPathId(PathId pathId);
}
