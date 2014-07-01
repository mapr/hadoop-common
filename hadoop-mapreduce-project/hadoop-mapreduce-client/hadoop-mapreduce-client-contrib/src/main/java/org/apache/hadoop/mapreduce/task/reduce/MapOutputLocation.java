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
}
