package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle.ShuffleError;
import org.apache.hadoop.util.Progress;

public class DirectShuffle<K,V> implements ShuffleConsumerPlugin<K, V>,
		ExceptionReporter {

	private static final int PROGRESS_FREQUENCY = 2000;
    private static final int MAX_EVENTS_TO_FETCH = 10000;
    private static final int MIN_EVENTS_TO_FETCH = 100;
    private static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;

	private org.apache.hadoop.mapred.ShuffleConsumerPlugin.Context<K, V> context;
	private TaskAttemptID reduceId;
	private JobConf jobConf;
	private TaskUmbilicalProtocol umbilical;
	private Reporter reporter;
	private ShuffleClientMetrics metrics;
	private Progress copyPhase;
	private TaskStatus taskStatus;
	private Task reduceTask;
	private DirectShuffleSchedulerImpl<K,V> scheduler;
	private MergeManager<K, V> merger;
	private Throwable throwable = null;
	private String throwingThreadName = null;



	@Override
	public void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
	}

	@Override
	public void init(ShuffleConsumerPlugin.Context<K, V> context) {
	    this.context = context;

	    this.reduceId = context.getReduceId();
	    this.jobConf = context.getJobConf();
	    this.umbilical = context.getUmbilical();
	    this.reporter = context.getReporter();
	    // TODO - may need to have our own ClientMetrics
	    this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
	    this.copyPhase = context.getCopyPhase();
	    this.taskStatus = context.getStatus();
	    this.reduceTask = context.getReduceTask();
	    
	    scheduler = new DirectShuffleSchedulerImpl<K, V>(jobConf, taskStatus, reduceId,
	        this, copyPhase, context.getShuffledMapsCounter(),
	        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());
	    try {
        merger = createMergeManager(this.context);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
	}

	@Override
	public RawKeyValueIterator run() throws IOException, InterruptedException {
		//fetchOutputs();
	    int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
	            MAX_RPC_OUTSTANDING_EVENTS / jobConf.getNumReduceTasks());
	        int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);

	        // Start the map-completion events fetcher thread
	        final DirectShuffleEventFetcher<K,V> eventFetcher = 
	          new DirectShuffleEventFetcher<K,V>(reduceId, umbilical, scheduler, this,
	              maxEventsToFetch);
	        eventFetcher.start();
	        
	        // Start the map-output fetcher threads
	        final int numFetchers = jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
	        DirectShuffleFetcher<K,V>[] fetchers = new DirectShuffleFetcher[numFetchers];
	        for (int i=0; i < numFetchers; ++i) {
	          fetchers[i] = new DirectShuffleFetcher<K,V>(jobConf, reduceId, scheduler, merger, 
	                                         reporter, metrics, this, context.getMapOutputFile());
	          fetchers[i].start();
	        }
	        
	        // Wait for shuffle to complete successfully
	        while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
	          reporter.progress();
	          
	          synchronized (this) {
	            if (throwable != null) {
	              throw new ShuffleError("error in shuffle in " + throwingThreadName,
	                                     throwable);
	            }
	          }
	        }

	        // Stop the event-fetcher thread
	        eventFetcher.shutDown();
	        
	        // Stop the map-output fetcher threads
	        for (DirectShuffleFetcher<K,V> fetcher : fetchers) {
	          fetcher.shutDown();
	        }
	        
	    // stop the scheduler
	    scheduler.close();

	    copyPhase.complete(); // copy is already complete
	    taskStatus.setPhase(TaskStatus.Phase.SORT);
	    reduceTask.statusUpdate(umbilical);

	    // Finish the on-going merges...
	    RawKeyValueIterator kvIter = null;
	    try {
	      kvIter = merger.close();
	    } catch (Throwable e) {
	      throw new ShuffleError("Error while doing final merge " , e);
	    }

	    // Sanity check
	    synchronized (this) {
	      if (throwable != null) {
	        throw new ShuffleError("error in shuffle in " + throwingThreadName,
	                               throwable);
	      }
	    }
	    
	    return kvIter;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	protected MergeManager<K, V> createMergeManager(
		      ShuffleConsumerPlugin.Context<K,V> context) throws IOException {
		    return new DirectShuffleMergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
		        reporter, context.getCodec(),
		        context.getCombinerClass(), context.getCombineCollector(), 
		        context.getSpilledRecordsCounter(),
		        context.getReduceCombineInputCounter(),
		        context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
		        context.getMapOutputFile());
	}
	/*
	public boolean fetchOutputs() throws IOException {
	      int totalFailures = 0;
	      DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
	      final Progress copyPhase = 
	        reduceTask.getProgress().phase();
	      LocalFSMerger localFSMergerThread = null;
	      InMemFSMergeThread inMemFSMergeThread = null;
	      GetMapEventsThread getMapEventsThread = null;
	      
	      for (int i = 0; i < numMaps; i++) {
	        copyPhase.addPhase();       // add sub-phase per file
	      }
	      
	      // start the map events thread
	      getMapEventsThread = new GetMapEventsThread();
	      getMapEventsThread.start();
	      
	      copiers = new ArrayList<MapOutputCopier>(numCopiers);
	      
	      // start all the copying threads
	      for (int i=0; i < numCopiers; i++) {
	        MapOutputCopier copier = new MapOutputCopier(conf, reporter, 
	            reduceTask.getJobTokenSecret());
	        copiers.add(copier);
	        copier.start();
	      }
	      
	      //start the on-disk-merge thread
	      localFSMergerThread = new LocalFSMerger((FileSystem)localFileSys);
	      //start the in memory merger thread
	      inMemFSMergeThread = new InMemFSMergeThread();
	      localFSMergerThread.start();
	      inMemFSMergeThread.start();
	      
	      // start the clock for bandwidth measurement
	      long startTime = System.currentTimeMillis();
	      long currentTime = startTime;
	      long lastProgressTime = startTime;
	      long lastOutputTime = 0;
	      
	        // loop until we get all required outputs
	        while (copiedMapOutputs.size() < numMaps && mergeThrowable == null) {
	          currentTime = System.currentTimeMillis();
	          boolean logNow = false;
	          if (currentTime - lastOutputTime > MIN_LOG_TIME) {
	            lastOutputTime = currentTime;
	            logNow = true;
	          }
	          if (logNow) {
	            if (LOG.isInfoEnabled()) {
	              LOG.info(reduceTask.getTaskID() + " Need another "
	                     + (numMaps - copiedMapOutputs.size()) + " map output(s)");
	            }
	          }
	          if (retryFetches.size() > 0) {
	            scheduleNewMaps(retryFetches);
	            if (LOG.isInfoEnabled()) {
	              LOG.info(reduceTask.getTaskID() + ": " +  
	                    "Got " + retryFetches.size() +
	                    " map-outputs from previous failures");
	            }
	          }

	          while (copiedMapOutputs.size() < numMaps 
	              && retryFetches.size() == 0
	              && mergeThrowable == null)
	          {
	            final LinkedList<CopyResult> crList = getCopyResults();

	            for (final Iterator<CopyResult> crit = crList.iterator();
	                 crit.hasNext(); )
	            {
	              final CopyResult cr = crit.next();
	              crit.remove();

	              if (cr.getSuccess()) {  // a successful copy
	                numCopied++;
	                lastProgressTime = System.currentTimeMillis();
	                reduceShuffleBytes.increment(cr.getSize());
	                  
	                long secsSinceStart = 
	                  (System.currentTimeMillis()-startTime)/1000+1;
	                float mbs = ((float)reduceShuffleBytes.getCounter())/(1024*1024);
	                float transferRate = mbs/secsSinceStart;
	                copyPhase.startNextPhase();
	                copyPhase.setStatus("copy (" + numCopied + " of " + numMaps 
	                                    + " at " +
	                                    mbpsFormat.format(transferRate) +  " MB/s)");
	                // Note successful fetch for this mapId to invalidate
	                // (possibly) old fetch-failures
	                fetchFailedMaps.remove(cr.getLocation().getTaskId());
	              } else if (cr.isObsolete()) {
	                //ignore
	                LOG.info(reduceTask.getTaskID() + 
	                         " Ignoring obsolete copy result for Map Task: " + 
	                         cr.getLocation().getTaskAttemptId() + " from host: " + 
	                         cr.getHost());
	              } else {
	                retryFetches.add(cr.getLocation());
	                
	                // note the failed-fetch
	                TaskAttemptID mapTaskId = cr.getLocation().getTaskAttemptId();
	                TaskID mapId = cr.getLocation().getTaskId();
	                
	                totalFailures++;
	                Integer noFailedFetches = 
	                  mapTaskToFailedFetchesMap.get(mapTaskId);
	                noFailedFetches = 
	                  (noFailedFetches == null) ? 1 : (noFailedFetches + 1);
	                mapTaskToFailedFetchesMap.put(mapTaskId, noFailedFetches);
	                LOG.info("Task " + getTaskID() + ": Failed fetch #" + 
	                         noFailedFetches + " from " + mapTaskId);

	                // half the number of max fetch retries per map during 
	                // the end of shuffle
	                int fetchRetriesPerMap = maxFetchRetriesPerMap;
	                int pendingCopies = numMaps - numCopied;
	                
	                // The check noFailedFetches != maxFetchRetriesPerMap is
	                // required to make sure of the notification in case of a
	                // corner case : 
	                // when noFailedFetches reached maxFetchRetriesPerMap and 
	                // reducer reached the end of shuffle, then we may miss sending
	                // a notification if the difference between 
	                // noFailedFetches and fetchRetriesPerMap is not divisible by 2 
	                if (pendingCopies <= numMaps * MIN_PENDING_MAPS_PERCENT &&
	                    noFailedFetches != maxFetchRetriesPerMap) {
	                  fetchRetriesPerMap = fetchRetriesPerMap >> 1;
	                }
	                
	                // did the fetch fail too many times?
	                // using a hybrid technique for notifying the jobtracker.
	                //   a. the first notification is sent after max-retries 
	                //   b. subsequent notifications are sent after 2 retries.   
	                //   c. send notification immediately if it is a read error and 
	                //       "mapreduce.reduce.shuffle.notify.readerror" set true.   
	                boolean isReadError = cr.getError().equals(CopyOutputErrorType.READ_ERROR);
	                if ((reportReadErrorImmediately && isReadError) ||
	                   ((noFailedFetches >= fetchRetriesPerMap) 
	                    && ((noFailedFetches - fetchRetriesPerMap) % 2) == 0)) {
	                  synchronized (ReduceTask.this) {
	                    taskStatus.addFetchFailedMap(mapTaskId);
	                    reporter.progress();
	                    LOG.info("Failed to fetch map-output from " + mapTaskId + 
	                             " even after " + noFailedFetches + " tries... "
	                             + (isReadError ? "and it is a read error, " : "")
	                             + "reporting to the JobTracker");
	                  }
	                }
	                // note unique failed-fetch maps
	                if (noFailedFetches == maxFetchRetriesPerMap) {
	                  fetchFailedMaps.add(mapId);
	                    
	                  // did we have too many unique failed-fetch maps?
	                  // and did we fail on too many fetch attempts?
	                  // and did we progress enough
	                  //     or did we wait for too long without any progress?
	                 
	                  // check if the reducer is healthy
	                  boolean reducerHealthy = 
	                      (((float)totalFailures / (totalFailures + numCopied)) 
	                       < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
	                  
	                  // check if the reducer has progressed enough
	                  boolean reducerProgressedEnough = 
	                      (((float)numCopied / numMaps) 
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
	                      (shuffleProgressDuration > maxMapRuntime) 
	                      ? shuffleProgressDuration 
	                      : maxMapRuntime;
	                  boolean reducerStalled = 
	                      (((float)stallDuration / minShuffleRunDuration) 
	                       >= MAX_ALLOWED_STALL_TIME_PERCENT);
	                  
	                  // kill if not healthy and has insufficient progress
	                  if ((fetchFailedMaps.size() >= maxFailedUniqueFetches ||
	                       fetchFailedMaps.size() == (numMaps - copiedMapOutputs.size()))
	                      && !reducerHealthy 
	                      && (!reducerProgressedEnough || reducerStalled))
	                  {
	                    LOG.fatal("Shuffle failed with too many fetch failures " + 
	                              "and insufficient progress!" +
	                              "Killing task " + getTaskID() + ".");
	                    umbilical.shuffleError(getTaskID(), 
	                                           "Exceeded MAX_FAILED_UNIQUE_FETCHES;"
	                                           + " bailing-out.", jvmContext);
	                  }
	                }
	                  
	                // back off exponentially until num_retries <= max_retries
	                // back off by max_backoff/2 on subsequent failed attempts
	                currentTime = System.currentTimeMillis();
	                int currentBackOff = noFailedFetches <= fetchRetriesPerMap 
	                                     ? BACKOFF_INIT 
	                                       * (1 << (noFailedFetches - 1)) 
	                                     : (this.maxBackoff * 1000 / 2);
	                // If it is read error,
	                //    back off for maxMapRuntime/2
	                //    during end of shuffle, 
	                //      backoff for min(maxMapRuntime/2, currentBackOff) 
	                if (cr.getError().equals(CopyOutputErrorType.READ_ERROR)) {
	                  int backOff = maxMapRuntime >> 1;
	                  if (pendingCopies <= numMaps * MIN_PENDING_MAPS_PERCENT) {
	                    backOff = Math.min(backOff, currentBackOff); 
	                  } 
	                  currentBackOff = backOff;
	                }

	              }
	            }
	          }
	        }
	        
	        // all done, inform the copiers to exit
	        exitGetMapEvents= true;
	        try {
	          getMapEventsThread.join();
	          LOG.info("getMapsEventsThread joined.");
	        } catch (InterruptedException ie) {
	          LOG.info("getMapsEventsThread threw an exception: " +
	              StringUtils.stringifyException(ie));
	        }

	        for (MapOutputCopier copier : copiers) {
	          copier.shutdown();
	        }
	        copiers.clear();
	        
	        // copiers are done, exit and notify the waiting merge threads
	        synchronized (mapOutputFilesOnDisk) {
	          exitLocalFSMerge = true;
	          mapOutputFilesOnDisk.notify();
	        }
	        
	        ramManager.close();
	        
	        //Do a merge of in-memory files (if there are any)
	        if (mergeThrowable == null) {
	          try {
	            // Wait for the on-disk merge to complete
	            localFSMergerThread.join();
	            LOG.info("Interleaved on-disk merge complete: " + 
	                     mapOutputFilesOnDisk.size() + " files left.");
	            
	            //wait for an ongoing merge (if it is in flight) to complete
	            inMemFSMergeThread.join();
	            LOG.info("In-memory merge complete: " + 
	                     mapOutputsFilesInMemory.size() + " files left.");
	            } catch (InterruptedException ie) {
	            LOG.warn(reduceTask.getTaskID() +
	                     " Final merge of the inmemory files threw an exception: " + 
	                     StringUtils.stringifyException(ie));
	            // check if the last merge generated an error
	            if (mergeThrowable != null) {
	              mergeThrowable = ie;
	            }
	            return false;
	          }
	        }
	        return mergeThrowable == null && copiedMapOutputs.size() == numMaps;
	    }
	    */

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
}
