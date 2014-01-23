package org.apache.hadoop.mapred;

import java.text.Format;
import java.util.Collection;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;

public class FetchCounters {

  private static final Log LOG = LogFactory.getLog(GetCounters.class.getName());
  
  JobTracker jobTracker;
  
  public FetchCounters(JobTracker jobtracker) {
    this.jobTracker = jobtracker;
  }
  
  public HashMap<String, Number> getCounterMap() {
    HashMap<String, Number> counter = new HashMap<String, Number>();
    ClusterMetrics clusterMetrics = jobTracker.getClusterMetrics();
    Vector<JobInProgress> runningJobsVector = jobTracker.runningJobs();
    int noOfRunningJobs = runningJobsVector.size();
    counter.put("Counter.MapReduceFramework.NumJobsRunning", noOfRunningJobs);
    counter.put("Counter.MapReduceFramework.NumJobsFailed", jobTracker.failedJobs().size());
    counter.put("mapred.jobtracker.TotalTaskTrackers", jobTracker.taskTrackers().size());
    
    try {
      counter.put("Counter.MapReduceFramework.NumMapSlotsOpen",
          clusterMetrics.getMapSlotCapacity() - clusterMetrics.getOccupiedMapSlots());
      counter.put("Counter.MapReduceFramework.NumReduceSlotsOpen",
          clusterMetrics.getReduceSlotCapacity() - clusterMetrics.getOccupiedReduceSlots());
      counter.put("mapred.jobtracker.NoOfLiveTaskTrackers", clusterMetrics.getTaskTrackerCount());
      counter.put("mapred.jobtracker.NoOfBlackListedTaskTrackers", clusterMetrics.getBlackListedTaskTrackerCount());
      // counter.put("mapred.jobtracker.NoOfGrayListedTaskTrackers", clusterMetrics.getGrayListedTaskTrackerCount());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    int totalMapTasksRemaining = 0;
    int totalReduceTasksRemaining = 0;
    
    //Collecting job specific counters
    long totalHdfsRead = 0, totalHdfsWrite = 0, totalS3Read = 0, totalS3Write = 0;
    for(JobInProgress job : runningJobsVector) {
      TaskInProgress[] mapTasks = job.getTasks(TaskType.MAP);
      TaskInProgress[] reduceTasks = job.getTasks(TaskType.REDUCE);
      
      //counters for Map Tasks
      int totalTasks = mapTasks.length;
      int runningTasks = 0;
      int finishedTasks = 0;
      int killedTasks = 0;
      int failedTaskAttempts = 0;
      int killedTaskAttempts = 0;
      for(int i=0; i < totalTasks; ++i) {
        TaskInProgress task = mapTasks[i];
        if (task.isComplete()) {
          finishedTasks += 1;
        } else if (task.isRunning()) {
          runningTasks += 1;
        } else if (task.wasKilled()) {
          killedTasks += 1;
        }
        failedTaskAttempts += task.numTaskFailures();
        killedTaskAttempts += task.numKilledTasks();
      }
      int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
      //counter.put("Counter.MapReduceFramework." + job.getJobID() + ".TotalMapTasks", totalTasks);
      counter.put("Counter.MapReduceFramework." + job.getJobID() + ".RunningMapTasks", runningTasks);
      counter.put("Counter.MapReduceFramework." + job.getJobID() + ".RemainingMapTasks", pendingTasks);
      totalMapTasksRemaining += pendingTasks;
      
      
      //counters for Reduce tasks
      totalTasks = reduceTasks.length;
      runningTasks = 0;
      finishedTasks = 0;
      killedTasks = 0;
      failedTaskAttempts = 0;
      killedTaskAttempts = 0;
      for(int i=0; i < totalTasks; ++i) {
        TaskInProgress task = reduceTasks[i];
        if (task.isComplete()) {
          finishedTasks += 1;
        } else if (task.isRunning()) {
          runningTasks += 1;
        } else if (task.wasKilled()) {
          killedTasks += 1;
        }
        failedTaskAttempts += task.numTaskFailures();
        killedTaskAttempts += task.numKilledTasks();
      }
      pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks;
      counter.put("Counter.MapReduceFramework." + job.getJobID() + ".RunningReduceTasks", runningTasks);
      counter.put("Counter.MapReduceFramework." + job.getJobID() + ".RemainingReduceTasks", pendingTasks);
      totalReduceTasksRemaining += pendingTasks;
    
      // TODO MapR - temp hack until Apache patches are pulled
      //
      // Counters totalCounters = new Counters();
      // boolean isFine = true;
      // isFine = job.getCounters(totalCounters);
      // totalCounters = (isFine? totalCounters: new Counters());
      //
      final Counters totalCounters = job.getCounters();

      for (String groupName : totalCounters.getGroupNames()) {
        Counters.Group totalGroup = totalCounters.getGroup(groupName);

        for (Counters.Counter counterRecord : totalGroup) {
          String name = counterRecord.getDisplayName();
          long totalValue = counterRecord.getCounter();
          if(name.equals("HDFS_BYTES_READ")) {
            counter.put("Counter.FileSystem." + job.getJobID() + "." + name, totalValue);
            LOG.info("Counter.FileSystem." + job.getJobID() + "." + name + totalValue);
            totalHdfsRead += totalValue;
          }
          if(name.equals("HDFS_BYTES_WRITTEN")) {
            counter.put("Counter.FileSystem." + job.getJobID() + "." + name, totalValue);
            LOG.info("Counter.FileSystem." + job.getJobID() + "." + name + totalValue);
            totalHdfsWrite += totalValue;
          }
          if(name.equals("S3_BYTES_READ") || name.equals("S3N_BYTES_READ")) {
            counter.put("Counter.FileSystem." + job.getJobID() + ".S3_BYTES_READ", totalValue);
            LOG.info("Counter.FileSystem." + job.getJobID() + "." + name + totalValue);
            totalS3Read += totalValue;
          }
          if(name.equals("S3_BYTES_WRITTEN") || name.equals("S3N_BYTES_WRITTEN")) {
            counter.put("Counter.FileSystem." + job.getJobID() + ".S3_BYTES_WRITTEN", totalValue);
            LOG.info("Counter.FileSystem." + job.getJobID() + "." + name + totalValue);
            totalS3Write += totalValue;
          }
          
        }
      }
      
    }
    
    counter.put("Counter.FileSystem.HDFS_BYTES_READ" , totalHdfsRead);
    counter.put("Counter.FileSystem.HDFS_BYTES_WRITTEN",totalHdfsWrite);
    counter.put("Counter.FileSystem.S3_BYTES_READ", totalS3Read);
    counter.put("Counter.FileSystem.S3_BYTES_WRITTEN", totalS3Write);
      
	  ClusterStatus status = jobTracker.getClusterStatus();
      
      if(status.getMaxMapTasks() != 0) {
        counter.put("Counter.MapReduceFramework.RemainingMapTasksPerSlot",
            totalMapTasksRemaining/(double)status.getMaxMapTasks());
      } else {
        counter.put("Counter.MapReduceFramework.RemainingMapTasksPerSlot", 0);
      }
      
      counter.put("Counter.MapReduceFramework.RemainingMapTasks", totalMapTasksRemaining);
      counter.put("Counter.MapReduceFramework.RemainingReduceTasks", totalReduceTasksRemaining);
      counter.put("Counter.MapReduceFramework.RunningMapTasks", status.getMapTasks());
      counter.put("Counter.MapReduceFramework.RunningReduceTasks", status.getReduceTasks());
    
    return counter;
  }
}
