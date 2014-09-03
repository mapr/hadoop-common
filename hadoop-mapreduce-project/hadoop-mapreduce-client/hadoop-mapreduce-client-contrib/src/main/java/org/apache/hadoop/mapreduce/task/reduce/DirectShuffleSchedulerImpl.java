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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.util.Progress;

public class DirectShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K, V> {

  private static final Log LOG = LogFactory.getLog(DirectShuffleSchedulerImpl.class);
  
  private static final long INITIAL_PENALTY = 10000;
  private static final float PENALTY_GROWTH_RATE = 1.3f;
  private final static int REPORT_FAILURE_LIMIT = 10;
  /**
   * Minimum number of map fetch retries.
   */
  //private static final int MIN_FETCH_RETRIES_PER_MAP = 2;
  /**
   * Initial backoff interval (milliseconds)
   */
  //private static final int BACKOFF_INIT = 4000;


  private final LinkedList<MapOutputLocation> newMapOutputs =
      new LinkedList<MapOutputLocation>();
  
  // Needs to be synchronized???
  private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();
  private final boolean[] finishedMaps;
  private final int totalMaps;
  private int remainingMaps;

  private final TaskAttemptID reduceId;

  private final TaskStatus status;
  private final ExceptionReporter reporter;
  private Configuration conf;
  private final int abortFailureLimit;
  private final Progress progress;
  private final Map<TaskAttemptID,IntWritable> failureCounts =
		    new HashMap<TaskAttemptID,IntWritable>();
  private final Counters.Counter shuffledMapsCounter;
  private final Counters.Counter reduceShuffleBytes;
  private final Counters.Counter failedShuffleCounter;

  private final long startTime;
  private long lastProgressTime;

  private volatile int maxMapRuntime = 0;
  /**
   * Maximum number of fetch-retries per-map.
   */
  private final int maxFailedUniqueFetches;
  private final int maxFetchFailuresBeforeReporting;
  private final Random random = new Random();

  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final boolean reportReadErrorImmediately;
  private long maxDelay = MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY;

  private final Map<String,IntWritable> hostFailures =
    new HashMap<String,IntWritable>();


  
  public DirectShuffleSchedulerImpl(JobConf job, TaskStatus status,
                          TaskAttemptID reduceId,
                          ExceptionReporter reporter,
                          Progress progress,
                          Counters.Counter shuffledMapsCounter,
                          Counters.Counter reduceShuffleBytes,
                          Counters.Counter failedShuffleCounter) {
    totalMaps = job.getNumMapTasks();
    abortFailureLimit = Math.max(30, totalMaps / 10);
    conf = job;
    remainingMaps = totalMaps;
    finishedMaps = new boolean[remainingMaps];
    this.reporter = reporter;
    this.status = status;
    this.reduceId = reduceId;
    this.progress = progress;
    this.shuffledMapsCounter = shuffledMapsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.failedShuffleCounter = failedShuffleCounter;
    this.startTime = System.currentTimeMillis();
    lastProgressTime = startTime;
   // referee.start();
    this.maxFailedUniqueFetches = Math.min(totalMaps, 5);
    this.maxFetchFailuresBeforeReporting = job.getInt(
        MRJobConfig.SHUFFLE_FETCH_FAILURES, REPORT_FAILURE_LIMIT);
    this.reportReadErrorImmediately = job.getBoolean(
        MRJobConfig.SHUFFLE_NOTIFY_READERROR, true);

    this.maxDelay = job.getLong(MRJobConfig.MAX_SHUFFLE_FETCH_RETRY_DELAY,
        MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY);

    
  }


@Override
  public synchronized boolean waitUntilDone(int millis) throws InterruptedException {
    if (remainingMaps > 0) {
        wait(millis);
        return remainingMaps == 0;
      }
      return true;
  }

  @Override
  public void resolve(TaskCompletionEvent event) throws IOException,
      InterruptedException {
    // ignore non-map events
    if (!event.isMapTask()) {
      return;
    }
    switch (event.getTaskStatus()) {
      case SUCCEEDED:
      {
        TaskAttemptID taskId = event.getTaskAttemptId();
        Map<String, ByteBuffer> servicedata = event.getServiceMetaData();
        ByteBuffer directShuffleData = servicedata.get("mapr_direct_shuffle");
        PathId pathId = null;
        String host = null;
        try {
          if ( directShuffleData != null) {
              DataInputByteBuffer in = new DataInputByteBuffer();
              in.reset(directShuffleData);
              host = WritableUtils.readString(in);
              if (host == null) {
                throw new IOException("null hostname found in taskCompletionEvent" +
                   " location: '" +
                   event.getTaskTrackerHttp() +
                   "'");
              }
              int size = WritableUtils.readVInt(in);
              for (int i = 0; i < size; i++) {
                String dirName = WritableUtils.readString(in);
                PathId pathIdTmp = FileSystem.get(conf).createPathId();
                pathIdTmp.readFields(in);
                if ( dirName.equalsIgnoreCase(".")) {
                  pathId = pathIdTmp;
                }
              }
              in.close();
          } else {
            throw new IOException("No mapr_direct_shuffle service info was passed with taskCompletionEvent" +
                " location: '" +
                event.getTaskTrackerHttp() +
                "'");
          }
        } catch(Throwable t) {
          throw new IOException("No parentFid info was passed with taskCompletionEvent" +
              " location: '" +
              event.getTaskTrackerHttp() +
              "'");
        }
        int duration = event.getTaskRunTime();
        addKnownMapOutput(host,
            taskId,
            pathId);
        if (duration > maxMapRuntime) {
          maxMapRuntime = duration;
          // adjust max-fetch-retries based on max-map-run-time
          //maxFetchRetriesPerMap = Math.max(MIN_FETCH_RETRIES_PER_MAP,
          //  getClosestPowerOf2((maxMapRuntime / BACKOFF_INIT) + 1));
        }
      }
      break;
      case FAILED:
      case KILLED:
      case OBSOLETE:
      {
        obsoleteMapOutput(event.getTaskAttemptId());
        LOG.info("Ignoring obsolete output of " + event.getTaskStatus() +
            " map-task: '" + event.getTaskAttemptId() + "'");
      }
      break;
      case TIPFAILED:
      {
        tipFailed(event.getTaskAttemptId().getTaskID());
        LOG.info("Ignoring output of failed map TIP: '" +
            event.getTaskAttemptId() + "'");
        break;
      }
    }
  }

  public synchronized void addKnownMapOutput(String host, TaskAttemptID taskId,
      PathId pathId) {
    newMapOutputs.add(
        new MapOutputLocation(taskId, host, pathId));
    notifyAll();
  }

  public synchronized void obsoleteMapOutput(TaskAttemptID mapId) {
    obsoleteMaps.add(mapId);
  }
  
  public synchronized void tipFailed(TaskID taskId) {
    if (!finishedMaps[taskId.getId()]) {
      finishedMaps[taskId.getId()] = true;
      if (--remainingMaps == 0) {
        notifyAll();
      }
      updateStatus();
    }
  }
  
  public synchronized void copySucceeded(TaskAttemptID mapId,
		  MapOutputLocation loc,
          long bytes,
          long millis,
          MapOutput<K,V> output
          ) throws IOException {
  	failureCounts.remove(mapId);
  	hostFailures.remove(loc.getHost());
  	int mapIndex = mapId.getTaskID().getId();
  	
  	if (!finishedMaps[mapIndex]) {
    	output.commit();
    	finishedMaps[mapIndex] = true;
    	shuffledMapsCounter.increment(1);
    	if (--remainingMaps == 0) {
    	  notifyAll();
    	}
    	
    	// update the status
    	totalBytesShuffledTillNow += bytes;
    	updateStatus();
    	reduceShuffleBytes.increment(bytes);
    	lastProgressTime = System.currentTimeMillis();
    	LOG.debug("map " + mapId + " done " + status.getStateString());
  	}
	}
  
  public synchronized void copyFailed(TaskAttemptID mapId, MapOutputLocation loc) {
  	int failures = 1;
  	if (failureCounts.containsKey(mapId)) {
  		IntWritable x = failureCounts.get(mapId);
  		x.set(x.get() + 1);
  		failures = x.get();
  	} else {
  		failureCounts.put(mapId, new IntWritable(1));
  	}
  	String hostname = loc.getHost();
  	if (hostFailures.containsKey(hostname)) {
  		IntWritable x = hostFailures.get(hostname);
  		x.set(x.get() + 1);
  	} else {
  		hostFailures.put(hostname, new IntWritable(1));
  	}
  	if (failures >= abortFailureLimit) {
  		try {
  			throw new IOException(failures + " failures downloading " + mapId);
  		} catch (IOException ie) {
  			reporter.reportException(ie);
  		}
  	}
  
  	checkAndInformJobTracker(failures, mapId);
  	
  	checkReducerHealth();
  	
  	long delay = (long) (INITIAL_PENALTY *
  	Math.pow(PENALTY_GROWTH_RATE, failures));
  	if (delay > maxDelay) {
  		delay = maxDelay;
  	}
  
  	//penalties.add(new Penalty(host, delay));
  
  	failedShuffleCounter.increment(1);
  }

  private void updateStatus() {
    float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
    int mapsDone = totalMaps - remainingMaps;
    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

    float transferRate = mbs / secsSinceStart;
    progress.set((float) mapsDone / totalMaps);
    String statusString = mapsDone + " / " + totalMaps + " copied.";
    status.setStateString(statusString);

    progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
        + mbpsFormat.format(transferRate) + " MB/s)");
  }

  public static int getClosestPowerOf2(int value) {
    if (value <= 0)
      throw new IllegalArgumentException("Undefined for " + value);
    final int hob = Integer.highestOneBit(value);
    return Integer.numberOfTrailingZeros(hob) +
      (((hob >>> 1) & value) == 0 ? 0 : 1);
  }
  
  @Override
  public void close() throws InterruptedException {
    
  }

  public void reportLocalError(IOException ioe) {
    try {
      LOG.error("Shuffle failed : local error on this node: "
          + InetAddress.getLocalHost());
    } catch (UnknownHostException e) {
      LOG.error("Shuffle failed : local error on this node");
    }
    reporter.reportException(ioe);
  }

  // Notify the JobTracker
  // after every read error, if 'reportReadErrorImmediately' is true or
  // after every 'maxFetchFailuresBeforeReporting' failures
  private void checkAndInformJobTracker(
      int failures, TaskAttemptID mapId) {
    if ((reportReadErrorImmediately)
        || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
      LOG.info("Reporting fetch failure for " + mapId + " to the caller: MR AppMaster.");
      status.addFetchFailedMap((org.apache.hadoop.mapred.TaskAttemptID) mapId);
    }
  }

  private void checkReducerHealth() {
	    final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;
	    final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;
	    final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

	    long totalFailures = failedShuffleCounter.getValue();
	    int doneMaps = totalMaps - remainingMaps;

	    boolean reducerHealthy =
	      (((float)totalFailures / (totalFailures + doneMaps))
	          < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

	    // check if the reducer has progressed enough
	    boolean reducerProgressedEnough =
	      (((float)doneMaps / totalMaps)
	          >= MIN_REQUIRED_PROGRESS_PERCENT);

	    // check if the reducer is stalled for a long time
	    // duration for which the reducer is stalled
	    int stallDuration =
	      (int)(System.currentTimeMillis() - lastProgressTime);

	    // duration for which the reducer ran with progress
	    int shuffleProgressDuration =
	      (int)(lastProgressTime - startTime);

	    // min time the reducer should run without getting killed
	    int minShuffleRunDuration =
	      Math.max(shuffleProgressDuration, maxMapRuntime);

	    boolean reducerStalled =
	      (((float)stallDuration / minShuffleRunDuration)
	          >= MAX_ALLOWED_STALL_TIME_PERCENT);

	    // kill if not healthy and has insufficient progress
	    if ((failureCounts.size() >= maxFailedUniqueFetches ||
	        failureCounts.size() == (totalMaps - doneMaps))
	        && !reducerHealthy
	        && (!reducerProgressedEnough || reducerStalled)) {
	      LOG.fatal("Shuffle failed with too many fetch failures " +
	      "and insufficient progress!");
	      String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
	      reporter.reportException(new IOException(errorMsg));
	    }

	  }


  public synchronized MapOutputLocation getLocation() throws InterruptedException {
    while(newMapOutputs.isEmpty()) {
      wait();
    }
    final int numScheduled = newMapOutputs.size();
    ListIterator<MapOutputLocation> it =
        newMapOutputs.listIterator(random.nextInt(numScheduled));

    for (boolean wrapped = false; it.hasNext();) {
      MapOutputLocation loc = it.next();
      it.remove();
      if (!wrapped && !it.hasNext()) {
        wrapped = true;
        it = newMapOutputs.listIterator(0);
      }
      TaskAttemptID id = loc.getTaskAttemptId();
      if (obsoleteMaps.contains(id) || finishedMaps[id.getTaskID().getId()]) {
        LOG.info(reduceId +
            " Ignoring obsolete copy result for Map Task: " +
            loc.getTaskAttemptId() + " from host: " +
            loc.getHost());
        continue;
      }
      return loc;
    }
/*    MapOutputLocation loc = null;
    Iterator<MapOutputLocation> iter = newMapOutputs.iterator();
    int numToPick = random.nextInt(newMapOutputs.size());
    for (int i=0; i <= numToPick; ++i) {
      loc = iter.next();
    }
*/

  //  LOG.info("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
  //           " to " + Thread.currentThread().getName());
    //shuffleStart.set(System.currentTimeMillis());

    return null;

  }

}
