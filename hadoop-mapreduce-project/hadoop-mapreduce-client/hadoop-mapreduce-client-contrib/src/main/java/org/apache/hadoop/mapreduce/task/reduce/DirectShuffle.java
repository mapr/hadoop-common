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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    
    private static final Log LOG = LogFactory.getLog(DirectShuffle.class);

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
        LOG.error("Unable to create MergeManager", e);
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
	        DirectShuffleFetcher[] fetchers = new DirectShuffleFetcher[numFetchers];
	        for (int i=0; i < numFetchers; ++i) {
	          fetchers[i] = new DirectShuffleFetcher<K,V>(i, jobConf, reduceId, scheduler, merger,
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
}
