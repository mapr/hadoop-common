package org.apache.hadoop.mapreduce.task.reduce;

import java.io.FileNotFoundException;
import java.io.IOException;

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
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.MapRIFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.mapred.TaskAttemptID.*;

public class DirectShuffleFetcher<K, V> extends Thread {

  private static final Log LOG = LogFactory.getLog(DirectShuffleFetcher.class);

  private static enum ShuffleErrors{IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
    CONNECTION, WRONG_REDUCE}

  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";

  private final Counters.Counter ioErrs;

  private final TaskAttemptID reduceId;
  private MapOutputLocation currentLocation = null;
  private MapRFsOutputFile mapOutputFile;
  private int id;
  private Reporter reporter;
  private volatile boolean shouldExit = false;
  private boolean useDirectReduce = false;
  private int prefetchBytesHint;

  // Decompression of map-outputs
  private CompressionCodec codec = null;
  private Decompressor decompressor = null;
  private Configuration jobConf;
  private final MergeManager<K, V> merger;
  protected FileSystem rfs;
  private final ExceptionReporter exceptionReporter;

  private final ShuffleClientMetrics metrics;

  private DirectShuffleSchedulerImpl<K, V> scheduler;
  
  

  private static enum CopyOutputErrorType {
    NO_ERROR,
    READ_ERROR,
    OTHER_ERROR
  }

  private static final int OBSOLETE = -2;

  /**
   * Represents the result of an attempt to copy a map output
   */

  public DirectShuffleFetcher(int id, JobConf jobConf, TaskAttemptID reduceId,
                              DirectShuffleSchedulerImpl<K, V> scheduler, MergeManager<K, V> merger,
                              Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter exceptionReporter,
                              MapOutputFile mapOutputFile) {
    this.id = id;
    setName("MapOutputCopier " + reduceId.getTaskID() + "." + id);
    LOG.debug(getName() + " created");
    this.reporter = reporter;
    ioErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());

    this.exceptionReporter = exceptionReporter;
    this.reduceId = reduceId;
    this.mapOutputFile = (MapRFsOutputFile) mapOutputFile;
    this.metrics = metrics;

    this.jobConf = jobConf;
    this.merger = merger;
    try {
      this.rfs = FileSystem.get(jobConf);
    } catch (IOException e) {
      LOG.error("Unable to init filesystem", e);
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
   *
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

  public void shutDown() {
    shouldExit = true;
    // Send interrupt signal to shutdown the thread.
    this.interrupt();
  }

  /**
   * Loop forever and fetch map outputs as they become available.
   */
  @Override
  public void run() {
    try {
      while (!shouldExit ) {
        MapOutputLocation loc = null;
        try {
          // If merge is on, block
          merger.waitForResource();

          // Get a host to shuffle from
          loc = scheduler.getLocation();
          metrics.threadBusy();

          // Shuffle
          copyOutput(loc);
        } catch (IOException ioe) {
          LOG.warn(reduceId.getTaskID() + " copy failed: " +
            loc.getTaskAttemptId() + " from " + loc.getHost(),
            ioe);
          metrics.failedFetch();
        } catch (InterruptedException ie) {
          if (shouldExit) {
            break; // ALL DONE
          } else {
            LOG.warn("Unexpected InterruptedException");
          }
        } finally {
          if (loc != null) {
            metrics.threadFree();
          }
        }
      }
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
    }


    if (decompressor != null) {
      CodecPool.returnDecompressor(decompressor);
    }

  }

  /**
   * Copies a a map output from a remote host.
   *
   * @throws IOException          if there is an error copying the file
   * @throws InterruptedException if the copier should give up
   */
  @VisibleForTesting
  protected long copyOutput(MapOutputLocation loc) throws IOException, InterruptedException {
    if (loc == null) {
      return OBSOLETE;
    }

    // a temp filename. If this file gets created in ramfs, we're fine,
    // else, we will check the localFS to find a suitable final location
    // for this path
    //TaskAttemptID reduceId = reduceTask.getTaskID();
    Path filename, tmpMapOutput;
    MapOutput<K, V> mapOutput = null;
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
        downgrade(reduceId));
      tmpMapOutput = new Path(filename + "-" + id);
      mapOutput = getMapOutputFromFile(loc, tmpMapOutput,
        reduceId.getTaskID().getId());
    }

    if (mapOutput == null) {
      scheduler.copyFailed(loc.getTaskAttemptId(), loc);
      scheduler.addKnownMapOutput(loc.getHost(), loc.getTaskAttemptId(), loc.getShuffleRootFid());
      return -1;
     // throw new IOException("Failed to fetch map-output for " +
     //   loc.getTaskAttemptId() + " from " +
      //  loc.getHost());
    }
    // The size of the map-output
    long bytes = mapOutput.getSize();

    // Special case: discard empty map-outputs
    if (bytes == 0) {
      mapOutput.abort();
    }

    return bytes;
  }

  /* Read map output from a file.
   * Used by MapRFs  
   */
  private MapOutput<K, V> getMapOutputFromFile(MapOutputLocation mapOutputLoc,
                                               Path filename, int reduce) {
    MapOutput<K, V> mapOutput = null;
    // rfs has filesystem object     
    final String inputFile = mapOutputLoc.shuffleRootFid.getFid()
      + Path.SEPARATOR
      + mapOutputFile.getRelOutputFile(
      downgrade(mapOutputLoc.getTaskAttemptId()),
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
            downgrade(mapOutputLoc.getTaskAttemptId()), reduce),
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
          downgrade(mapOutputLoc.getTaskAttemptId()),
          mapOutputLength, id);
      } catch (IOException ioe) {
        // kill this reduce attempt
        ioErrs.increment(1);
        // This will abort reducer
        scheduler.reportLocalError(ioe);
        return null;
      }
      
   // Check if we can shuffle *now* ...
      if (mapOutput == null) {
        LOG.error("mapOutput can not be returned as null from reserve");
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
        if (mapOutput instanceof DirectOnDiskMapOutput) {
          // it is not an elegant solution but sizes are different between inMemory and onDisk
          dataSize = fileSize;
        }
        mapOutput.shuffle(new MapHost(mapOutputLoc.getHost(), ""), input,
          dataSize, dataLength,
          metrics, reporter);
      } catch (java.lang.InternalError e) {
        LOG.warn("Failed to shuffle for fetcher#" + id, e);
        throw new IOException(e);
      }

      // Inform the shuffle scheduler
      long endTime = System.currentTimeMillis();
      scheduler.copySucceeded(mapOutput.getMapId(), mapOutputLoc, mapOutput.getSize(),
        endTime - startTime, mapOutput);
      // Note successful shuffle
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
      return null;
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
    } */ catch (IOException ioe) {
      LOG.error("IOException reading map output of task " +
        mapOutputLoc.getTaskAttemptId() +
        " for reduce " + reduce +
        " file path " + inputFile,
        ioe);
      ioErrs.increment(1);
      if ( mapOutput == null ) {
        // most likely error is during read operation
        return null;
      }
      mapOutput.abort();
      metrics.failedFetch();
       return null;
    } /*catch (Exception e) {
      LOG.error("Exception reading map output of task " +
        mapOutputLoc.getTaskAttemptId() +
        " for reduce " + reduce +
        " file path " + inputFile,
        e);
      mapOutput.abort();
      metrics.failedFetch();
      return null;
    } */finally {
      // close input 
      if (input != null) {
        IOUtils.cleanup(LOG, input);
      }
    }
    return mapOutput;
  }
}
