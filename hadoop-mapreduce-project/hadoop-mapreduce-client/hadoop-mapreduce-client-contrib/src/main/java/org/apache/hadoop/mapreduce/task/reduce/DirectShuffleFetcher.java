package org.apache.hadoop.mapreduce.task.reduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.MapRIFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.ReflectionUtils;

public class DirectShuffleFetcher<K,V> extends Thread {
	
	private static final Log LOG = LogFactory.getLog(DirectShuffleFetcher.class);
  
  private final int reduce;
  private final TaskAttemptID reduceId;
  private MapOutputLocation currentLocation = null;
  private MapRFsOutputFile mapOutputFile;
  private int id; // = nextMapOutputCopierId++;
  private Reporter reporter;
  private boolean readError = false;
  private volatile boolean shouldExit = false;
  private boolean useDirectReduce = false;
  private int prefetchBytesHint;

  // Decompression of map-outputs
  private CompressionCodec codec = null;
  private Decompressor decompressor = null;
  private Configuration jobConf;
  private MergeManager<K,V> merger; 
  private FileSystem rfs;
  private final ExceptionReporter exceptionReporter;

  private final ShuffleClientMetrics metrics;
  
		private DirectShuffleSchedulerImpl<K,V> scheduler;

	    private static enum CopyOutputErrorType {
	        NO_ERROR,
	        READ_ERROR,
	        OTHER_ERROR
	      };

	      /** Represents the result of an attempt to copy a map output */
	      private class CopyResult {

	        // the map output location against which a copy attempt was made
	        private final MapOutputLocation loc;

	        // the size of the file copied, -1 if the transfer failed
	        private final long size;

	        //a flag signifying whether a copy result is obsolete
	        private static final int OBSOLETE = -2;

	        private CopyOutputErrorType error = CopyOutputErrorType.NO_ERROR;
	        CopyResult(MapOutputLocation loc, long size) {
	          this.loc = loc;
	          this.size = size;
	        }

	        CopyResult(MapOutputLocation loc, long size, CopyOutputErrorType error) {
	          this.loc = loc;
	          this.size = size;
	          this.error = error;
	        }

	        public boolean getSuccess() { return size >= 0; }
	        public boolean isObsolete() {
	          return size == OBSOLETE;
	        }
	        public long getSize() { return size; }
	        public String getHost() { return loc.getHost(); }
	        public MapOutputLocation getLocation() { return loc; }
	        public CopyOutputErrorType getError() { return error; }
	      }


    public DirectShuffleFetcher(JobConf jobConf, TaskAttemptID reduceId,
		DirectShuffleSchedulerImpl<K, V> scheduler, MergeManager<K, V> merger,
		Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter exceptionReporter,
		MapOutputFile mapOutputFile) {
        setName("MapOutputCopier " + reduceId.getTaskID() + "." + id);
        LOG.debug(getName() + " created");
        this.reporter = reporter;
        this.exceptionReporter = exceptionReporter;
        this.reduce = reduceId.getTaskID().getId();
        this.reduceId = reduceId;
        this.mapOutputFile = (MapRFsOutputFile) mapOutputFile;
        this.metrics = metrics;
        
        this.jobConf = jobConf;
        this.merger = merger;
        try {
			this.rfs = FileSystem.get(jobConf);
  		} catch (IOException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
        this.scheduler = scheduler;

        this.prefetchBytesHint = jobConf.getInt("maprfs.openfid2.prefetch.bytes", 0);
          if (jobConf.getCompressMapOutput()) {
            Class<? extends CompressionCodec> codecClass =
              jobConf.getMapOutputCompressorClass(DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, jobConf);
            decompressor = CodecPool.getDecompressor(codec);
          }
}

	/**
     * Fail the current file that we are fetching
     * @return were we currently fetching?
     */
    public synchronized boolean fail() {
      if (currentLocation != null) {
        finish(-1, CopyOutputErrorType.OTHER_ERROR);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Get the current map output location.
     */
    public synchronized MapOutputLocation getLocation() {
      return currentLocation;
    }

    private synchronized void start(MapOutputLocation loc) {
      currentLocation = loc;
    }

    private synchronized void finish(long size, CopyOutputErrorType error) {
      if (currentLocation != null) {
        LOG.debug(getName() + " finishing " + currentLocation + " =" + size);
      //  synchronized (copyResultsLock) {
      //    copyResults.add(new CopyResult(currentLocation, size, error));
      //    copyResultsLock.notify();
       // }
        currentLocation = null;
      }
    }

    public void shutDown () {
      shouldExit = true;
      // Send interrupt signal to shutdown the thread.
      this.interrupt();
   }

   /** Loop forever and fetch map outputs as they become available.
    * The thread exits when it is interrupted by {@link ReduceTaskRunner}
    */
   @Override
   public void run() {
         try {
           while (!shouldExit /*&& !Thread.currentThread().isInterrupted()*/) {
             MapOutputLocation loc = null;
             long size = -1;
             CopyOutputErrorType error = CopyOutputErrorType.OTHER_ERROR;
             readError = false;

             try {
               // If merge is on, block
               merger.waitForResource();

               // Get a host to shuffle from
               loc = scheduler.getLocation();
               metrics.threadBusy();

               // Shuffle
               size = copyOutput(loc);
               metrics.successFetch();
               error = CopyOutputErrorType.NO_ERROR;
             } catch (IOException ioe) {
                 LOG.warn(reduceId.getTaskID() + " copy failed: " +
                     loc.getTaskAttemptId() + " from " + loc.getHost(),
                     ioe);
                 metrics.failedFetch();
                 if (readError) {
                   if (/*reportReadErrorImmediately == true && */LOG.isInfoEnabled()) {
                     LOG.info(reduceId.getTaskID() + " read error from " +
                           loc.getTaskAttemptId() + ". Reporting immediately.");
                   }
                   error = CopyOutputErrorType.READ_ERROR;
               }
             } catch (InterruptedException ie) {
               if (shouldExit) {
                 break; // ALL DONE
               } else {
                 LOG.warn ("Unexpected InterruptedException");
               }
             } finally {
               if (loc != null) {
                 //scheduler.freeHost(loc);
                 metrics.threadFree();            
               }
             }
           }
         } catch (Throwable t) {
           exceptionReporter.reportException(t);
         }

       // TODO Figure out what to do with those exceptions 
       /*catch (FSError e) {
         LOG.error("Task: " + reduceId.getTaskID() + " - FSError: " +
                   StringUtils.stringifyException(e));
         try {
           umbilical.fsError(reduceId.getTaskID(), e.getMessage(), jvmContext);
         } catch (IOException io) {
           LOG.error("Could not notify TT of FSError: " +
                   StringUtils.stringifyException(io));
         }
       } catch (Throwable th) {
         String msg = reduceId.getTaskID() + " : Map output copy failure : "
                      + StringUtils.stringifyException(th);
         //reportFatalError(reduceId.getTaskID(), th, msg);
       }
     }*/

     if (decompressor != null) {
       CodecPool.returnDecompressor(decompressor);
     }

   }

   /** Copies a a map output from a remote host, via HTTP. 
    * @param currentLocation the map output location to be copied
    * @return the path (fully qualified) of the copied file
    * @throws IOException if there is an error copying the file
    * @throws InterruptedException if the copier should give up
    */
   private long copyOutput(MapOutputLocation loc
       ) throws IOException, InterruptedException {
     if ( loc == null ) {
       
       return CopyResult.OBSOLETE;
     }
    // check if we still need to copy the output from this location
     if ( scheduler.getMap(loc) == null ) {
       return CopyResult.OBSOLETE;
     }
    
    
    // a temp filename. If this file gets created in ramfs, we're fine,
    // else, we will check the localFS to find a suitable final location
    // for this path
    //TaskAttemptID reduceId = reduceTask.getTaskID();
    Path filename, tmpMapOutput;
    MapOutput<K,V> mapOutput = null;
    if (useDirectReduce) {
	    // TODO gshegalov finish direct reduce fid
	/*    filename = new Path(
	    mapOutputFile.getRelOutputFile(org.apache.hadoop.mapred.TaskAttemptID.downgrade(loc.getTaskAttemptId()),
	    reduceId.getTaskID().getId()));
	    
	    long size = rfs.getFileStatus(filename).getLen();
	    mapOutput = new MapOutput(loc.getTaskId(),
	                   loc.getTaskAttemptId(),
	                   jobConf,
	                   rfs.makeQualified(filename),
	                   size); */
    } else {
	  // get file for writing
	  filename = mapOutputFile.getInputFile(loc.getTaskId().getId(),
			  org.apache.hadoop.mapred.TaskAttemptID.downgrade(reduceId));
	  tmpMapOutput = new Path(filename+"-" + id);
	  mapOutput = getMapOutputFromFile(loc, tmpMapOutput,
	                            reduceId.getTaskID().getId());
    }
	  
	
    if (mapOutput == null) {
      throw new IOException("Failed to fetch map-output for " +
                loc.getTaskAttemptId() + " from " +
                loc.getHost());
	
    }
    // The size of the map-output
    long bytes = mapOutput.getSize();

 /*   if ( scheduler.getMap(loc) == null ) {
      mapOutput.abort();
      return CopyResult.OBSOLETE;
    }
*/
    // Special case: discard empty map-outputs
    if (bytes == 0) {
        mapOutput.abort();

      // Note that we successfully copied the map-output
      noteCopiedMapOutput(loc.getTaskId());

      return bytes;
    }

    // Note that we successfully copied the map-output
    noteCopiedMapOutput(loc.getTaskId());

    return bytes;
  }

  /**
   * Save the map taskid whose output we just copied.
   * This function assumes that it has been synchronized on ReduceTask.this.
   * 
   * @param taskId map taskid
   */
  private void noteCopiedMapOutput(TaskID taskId) {
    //copiedMapOutputs.put(taskId, DUMMY_STRING);
    //ramManager.setNumCopiedMapOutputs(numMaps - copiedMapOutputs.size());
  }

  /* Read map output from a file.
   * Used by MapRFs  
   */
  private MapOutput<K,V> getMapOutputFromFile(MapOutputLocation mapOutputLoc,
                    Path filename, int reduce) {
    MapOutput<K,V> mapOutput = null;
    // rfs has filesystem object     
    final String inputFile = mapOutputLoc.shuffleRootFid.getFid()
      + Path.SEPARATOR
      + mapOutputFile.getRelOutputFile(
    		  org.apache.hadoop.mapred.TaskAttemptID.downgrade(mapOutputLoc.getTaskAttemptId()),
          reduce);

    FSDataInputStream input = null;
    long startTime = System.currentTimeMillis();
    try {
      {
        final long _st = LOG.isDebugEnabled()
          ? System.currentTimeMillis()
          : 0L;

        // open a file
        input = rfs.openFid2(
          mapOutputLoc.shuffleRootFid,
          mapOutputFile.getRelOutputFile(
        		  org.apache.hadoop.mapred.TaskAttemptID.downgrade(mapOutputLoc.getTaskAttemptId()), reduce),
          prefetchBytesHint);

        if (LOG.isDebugEnabled()) {
          LOG.debug("MapR-DBG: openFid2 " + input
                  + " took " + (System.currentTimeMillis() - _st));
        }
      }
      long fileSize = input.getFileLength();

      // create a mapr checksum stream
      MapRIFileInputStream checksumInput = new MapRIFileInputStream(input, fileSize, jobConf);
      long mapOutputLength = checksumInput.getMapOutputFileInfo().getMapOutputSize(); // map output size
      long dataLength = checksumInput.getMapOutputFileInfo().getFileBytesWritten(); // bytes written in file
      
      // Get the location for the map output - either in-memory or on-disk
      try {
        mapOutput = merger.reserve(
        		org.apache.hadoop.mapred.TaskAttemptID.downgrade(mapOutputLoc.getTaskAttemptId()), 
        		mapOutputLength, id);
      } catch (IOException ioe) {
        // kill this reduce attempt
        //ioErrs.increment(1);
        scheduler.reportLocalError(ioe);
        // TODO fix it
        return null;
      }
      
      // Check if we can shuffle *now* ...
      if (mapOutput == null) {
        LOG.info("fetcher#" + id + " - MergeManager returned status WAIT ...");
        //Not an error but wait to process data.
        // TODO handle this
        return null;
      } 
      // The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
      // on decompression failures. Catching and re-throwing as IOException
      // to allow fetch failure logic to be processed
      try {
        // Go!
        LOG.info("fetcher#" + id + " about to shuffle output of map "
            + mapOutput.getMapId() + " to " + mapOutput.getDescription());
        long dataSize = mapOutputLength;
        if ( mapOutput instanceof DirectOnDiskMapOutput ) {
          // it is not an elegant solution but sizes are different between inMemory and onDisk
          dataSize = fileSize;
        }
        mapOutput.shuffle(new MapHost(mapOutputLoc.getHost(), ""), input, 
            dataSize, dataLength,  
            metrics, reporter);
      } catch (java.lang.InternalError e) {
        LOG.warn("Failed to shuffle for fetcher#"+id, e);
        throw new IOException(e);
      }
      
      // Inform the shuffle scheduler
      long endTime = System.currentTimeMillis();
      scheduler.copySucceeded(mapOutput.getMapId(), mapOutputLoc, mapOutput.getSize(), 
                              endTime - startTime, mapOutput);
      // Note successful shuffle
      //remaining.remove(mapId);
      metrics.successFetch();

      {
        final long _st = LOG.isDebugEnabled()
          ? System.currentTimeMillis()
          : 0L;

        input.close();

        if (LOG.isDebugEnabled()) {
          LOG.debug("MapR-DBG: close " + input + " took "
                  + (System.currentTimeMillis() - _st));
        }
      }
      input = null;
    } catch (FileNotFoundException fnfe) {
      LOG.error("FileNotFoundException reading map output of task " +
                mapOutputLoc.getTaskAttemptId() +
                " for reduce " + reduce +
                " file path " + inputFile,
                fnfe);
      readError = true;
    } // TODO Deal with MapR specific Exception 
   /* catch (IOExceptionWithErrorCode ioe) {
      int errCode = ioe.getErrorCode();
      LOG.error("IOExceptionWithErrorCode reading map output of task " +
                mapOutputLoc.getTaskAttemptId() +
                " for reduce " + reduce +
                " file path " + inputFile +
                " error code " + errCode,
                ioe);
      if (errCode == Errno.ESTALE || errCode == Errno.ENOENT) {
        readError=true;
      }
      return null;
    } */catch (IOException ioe) {
      LOG.error("IOException reading map output of task " +
                mapOutputLoc.getTaskAttemptId() +
                " for reduce " + reduce +
                " file path " + inputFile,
                ioe);
      return null;
    } catch (Exception e) {
      LOG.error("Exception reading map output of task " +
               mapOutputLoc.getTaskAttemptId() +
               " for reduce " + reduce +
               " file path " + inputFile,
               e);
      return null;
    } finally {
      // close input 
      if (input != null) {
        IOUtils.cleanup(LOG, input);
      }
    }
    return mapOutput;
  }
}
