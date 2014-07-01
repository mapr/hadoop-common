package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapHost.State;
import org.apache.hadoop.util.Progress;

public class DirectShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K, V> {

  private static final Log LOG = LogFactory.getLog(DirectShuffleSchedulerImpl.class);
  
  private static final int MAX_MAPS_AT_ONCE = 20;
  private static final long INITIAL_PENALTY = 10000;
  private static final float PENALTY_GROWTH_RATE = 1.3f;
  private final static int REPORT_FAILURE_LIMIT = 10;
  /**
   * Minimum number of map fetch retries.
   */
  private static final int MIN_FETCH_RETRIES_PER_MAP = 2;
  /**
   * Initial backoff interval (milliseconds)
   */
  private static final int BACKOFF_INIT = 4000;


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
  private final int abortFailureLimit;
  private final Progress progress;
  private final Counters.Counter shuffledMapsCounter;
  private final Counters.Counter reduceShuffleBytes;
  private final Counters.Counter failedShuffleCounter;

  private final long startTime;
  private long lastProgressTime;

  private volatile int maxMapRuntime = 0;
  /**
   * Maximum number of fetch-retries per-map.
   */
  private volatile int maxFetchRetriesPerMap;
  private final int maxFailedUniqueFetches;
  private final int maxFetchFailuresBeforeReporting;

  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final boolean reportReadErrorImmediately;
  private long maxDelay = MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY;

  
  public DirectShuffleSchedulerImpl(JobConf job, TaskStatus status,
                          TaskAttemptID reduceId,
                          ExceptionReporter reporter,
                          Progress progress,
                          Counters.Counter shuffledMapsCounter,
                          Counters.Counter reduceShuffleBytes,
                          Counters.Counter failedShuffleCounter) {
    totalMaps = job.getNumMapTasks();
    abortFailureLimit = Math.max(30, totalMaps / 10);

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
  public boolean waitUntilDone(int millis) throws InterruptedException {
    // TODO Auto-generated method stub
    return false;
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
        URI u = URI.create(event.getTaskTrackerHttp());
        String host = u.getHost();
        if (host == null) {
          throw new IOException("Invalid hostname found in tracker" +
             " location: '" +
             event.getTaskTrackerHttp() +
             "'");
        }
        TaskAttemptID taskId = event.getTaskAttemptId();
        int duration = event.getTaskRunTime();
        addKnownMapOutput(host,
            taskId,
            event.getPathId());
        if (duration > maxMapRuntime) {
          maxMapRuntime = duration;
          // adjust max-fetch-retries based on max-map-run-time
          maxFetchRetriesPerMap = Math.max(MIN_FETCH_RETRIES_PER_MAP,
            getClosestPowerOf2((maxMapRuntime / BACKOFF_INIT) + 1));
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

//        copiedMapOutputs.put(event.getTaskAttemptId().getTaskID(),
 //         DUMMY_STRING);
      }
    }
  }

  private void addKnownMapOutput(String host, TaskAttemptID taskId,
      PathId pathId) {
    newMapOutputs.add(
        new MapOutputLocation(taskId, host, pathId));
    // TODO will need to notify real fetchers
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
    // TODO Auto-generated method stub
    
  }

  
}
