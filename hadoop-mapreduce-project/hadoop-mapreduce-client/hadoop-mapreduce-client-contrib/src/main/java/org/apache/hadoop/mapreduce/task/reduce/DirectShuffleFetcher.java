package org.apache.hadoop.mapreduce.task.reduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.nativeio.Errno;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

public class DirectShuffleFetcher<K,V> extends Thread {
	
	private static final Log LOG = LogFactory.getLog(DirectShuffleFetcher.class);
  // basic/unit connection timeout (in milliseconds)
  private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
  // default read timeout (in milliseconds)
  private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;
  private final int shuffleConnectionTimeout;
  private final int shuffleReadTimeout;

  private MapOutputLocation currentLocation = null;
  private int id = nextMapOutputCopierId++;
  private Reporter reporter;
  private boolean readError = false;
  private boolean shouldExit = false;

  // Decompression of map-outputs
  private CompressionCodec codec = null;
  private Decompressor decompressor = null;

  private final SecretKey jobTokenSecret;
  private final ShuffleClientMetrics metrics;


  public DirectShuffleFetcher(JobConf job, Reporter reporter, SecretKey jobTokenSecret) {
    setName("MapOutputCopier " + reduceTask.getTaskID() + "." + id);
    LOG.debug(getName() + " created");
    this.reporter = reporter;

    this.jobTokenSecret = jobTokenSecret;

    shuffleConnectionTimeout =
        job.getInt("mapreduce.reduce.shuffle.connect.timeout", STALLED_COPY_TIMEOUT);
      shuffleReadTimeout =
        job.getInt("mapreduce.reduce.shuffle.read.timeout", DEFAULT_READ_TIMEOUT);

      if (job.getCompressMapOutput()) {
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job);
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
        synchronized (copyResultsLock) {
          copyResults.add(new CopyResult(currentLocation, size, error));
          copyResultsLock.notify();
        }
        currentLocation = null;
      }
    }

    public void shutdown () {
      shouldExit = true;
      // Send interrupt signal to shutdown the thread.
      this.interrupt();
   }

   /** Loop forever and fetch map outputs as they become available.
    * The thread exits when it is interrupted by {@link ReduceTaskRunner}
    */
   @Override
   public void run() {
     copyloop:
     while (!shouldExit) {
       try {
         MapOutputLocation loc = null;
         long size = -1;

         synchronized (scheduledCopiesHeadLock) {
           if (scheduledCopiesHead.isEmpty()) {
             synchronized (scheduledCopiesTailLock) {
               while (scheduledCopiesTail.isEmpty()) {
                 try {
                   scheduledCopiesTailLock.wait();
                 } catch (InterruptedException e) {
                   if (shouldExit) {
                     break copyloop;
                   } // else { ignore spurious wakeups }
                 }
               }
               // assert !scheduledCopiesTail.isEmpty()
               scheduledCopiesHead = scheduledCopiesTail;
               scheduledCopiesTail = new LinkedList<MapOutputLocation>();
             }
           }
           // assert !scheduledCopiesHead.isEmpty()
           loc = scheduledCopiesHead.remove();
         }

         CopyOutputErrorType error = CopyOutputErrorType.OTHER_ERROR;
         readError = false;
         try {
           metrics.threadBusy();
           start(loc);
           size = copyOutput(loc);
           metrics.successFetch();
           error = CopyOutputErrorType.NO_ERROR;
         } catch (IOException e) {
           LOG.warn(reduceTask.getTaskID() + " copy failed: " +
                    loc.getTaskAttemptId() + " from " + loc.getHost(),
                    e);
           metrics.failedFetch();
           if (readError) {
             if (reportReadErrorImmediately == true && LOG.isInfoEnabled()) {
               LOG.info(reduceTask.getTaskID() + " read error from " +
                          loc.getTaskAttemptId() + ". Reporting immediately.");
             }
             error = CopyOutputErrorType.READ_ERROR;
           }
           // Reset 
           size = -1;
         } finally {
           metrics.threadFree();
           finish(size, error);
         }
       } catch (InterruptedException e) {
         if (shouldExit) {
           break; // ALL DONE
         } else {
           LOG.warn ("Unexpected InterruptedException");
         }
       } catch (FSError e) {
         LOG.error("Task: " + reduceTask.getTaskID() + " - FSError: " +
                   StringUtils.stringifyException(e));
         try {
           umbilical.fsError(reduceTask.getTaskID(), e.getMessage(), jvmContext);
         } catch (IOException io) {
           LOG.error("Could not notify TT of FSError: " +
                   StringUtils.stringifyException(io));
         }
       } catch (Throwable th) {
         String msg = getTaskID() + " : Map output copy failure : "
                      + StringUtils.stringifyException(th);
         reportFatalError(getTaskID(), th, msg);
       }
     }

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
    // check if we still need to copy the output from this location
    if (copiedMapOutputs.containsKey(loc.getTaskId()) ||
        obsoleteMapIds.containsKey(loc.getTaskAttemptId())) {
       return CopyResult.OBSOLETE;
    }
    
    // a temp filename. If this file gets created in ramfs, we're fine,
    // else, we will check the localFS to find a suitable final location
    // for this path
    TaskAttemptID reduceId = reduceTask.getTaskID();
    Path filename, tmpMapOutput;
    MapOutput mapOutput = null;
    if (useMapRFs) {
      if (useDirectReduce) {
        // TODO gshegalov finish direct reduce fid
        filename = new Path(
        mapOutputFile.getRelOutputFile(loc.getTaskAttemptId(),
        reduceId.getTaskID().getId()));
        
        long size = localFileSys.getFileStatus(filename).getLen();
        mapOutput = new MapOutput(loc.getTaskId(),
                       loc.getTaskAttemptId(),
                       conf,
                       localFileSys.makeQualified(filename),
                       size);
      } else {
      // get file for writing
      filename = mapOutputFile.getInputFileForWrite(loc.getTaskId(),
                                         reduceId, -1);
      tmpMapOutput = new Path(filename+"-" + id);
      mapOutput = getMapOutputFromFile(loc, tmpMapOutput,
                            reduceId.getTaskID().getId());
      }
    } else {
      filename =
      new Path(
      String.format(MapOutputFile.REDUCE_INPUT_FILE_FORMAT_STRING,
      TaskTracker.OUTPUT, loc.getTaskId().getId()));
      
      // Copy the map output to a temp file whose name is unique to this attempt 
      tmpMapOutput = new Path(filename+"-"+id);
      
      // Copy the map output
      mapOutput = getMapOutput(loc, tmpMapOutput,
                  reduceId.getTaskID().getId());
    }

    if (mapOutput == null) {
      throw new IOException("Failed to fetch map-output for " +
                            loc.getTaskAttemptId() + " from " +
                            loc.getHost());
    }

    // The size of the map-output
    long bytes = mapOutput.compressedSize;

    if (copiedMapOutputs.containsKey(loc.getTaskId())) {
      mapOutput.discard();
      return CopyResult.OBSOLETE;
    }

    // Special case: discard empty map-outputs
    if (bytes == 0) {
      try {
        mapOutput.discard();
      } catch (IOException ioe) {
        LOG.info("Couldn't discard output of " + loc.getTaskId());
      }

      // Note that we successfully copied the map-output
      noteCopiedMapOutput(loc.getTaskId());

      return bytes;
    }

    // Process map-output
    if (mapOutput.inMemory) {
      // Save it in the synchronized list of map-outputs
      mapOutputsFilesInMemory.add(mapOutput);
    } else {
      // Rename the temporary file to the final file; 
      // ensure it is on the same partition
      if (!useDirectReduce) {
        tmpMapOutput = mapOutput.file;
        filename = new Path(tmpMapOutput.getParent(), filename.getName());
        if (!localFileSys.rename(tmpMapOutput, filename)) {
          localFileSys.delete(tmpMapOutput, true);
          bytes = -1;
          throw new IOException("Failed to rename map output " +
              tmpMapOutput + " to " + filename);
        }
      }
      synchronized (mapOutputFilesOnDisk) {
        addToMapOutputFilesOnDisk(localFileSys.getFileStatus(filename));
      }
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
    copiedMapOutputs.put(taskId, DUMMY_STRING);
    ramManager.setNumCopiedMapOutputs(numMaps - copiedMapOutputs.size());
  }

  /* Read map output from a file.
   * Used by MapRFs  
   */
  private MapOutput getMapOutputFromFile(MapOutputLocation mapOutputLoc,
                    Path filename, int reduce) {
    MapOutput mapOutput = null;
    // rfs has filesystem object     
    final String inputFile = mapOutputLoc.shuffleRootFid.fid
      + Path.SEPARATOR
      + mapOutputFile.getRelOutputFile(mapOutputLoc.getTaskAttemptId(),
          reduce);

    FSDataInputStream input = null;
    try {
      {
        final long _st = LOG.isDebugEnabled()
          ? System.currentTimeMillis()
          : 0L;

        // open a file
        input = rfs.openFid2(
          mapOutputLoc.shuffleRootFid,
          mapOutputFile.getRelOutputFile(mapOutputLoc.getTaskAttemptId(), reduce),
          prefetchBytesHint);

        if (LOG.isDebugEnabled()) {
          LOG.debug("MapR-DBG: openFid2 " + input
                  + " took " + (System.currentTimeMillis() - _st));
        }
      }
      long fileSize = input.getFileLength();

      // create a mapr checksum stream
      MapRIFileInputStream checksumInput = new MapRIFileInputStream(input, fileSize);
      long mapOutputLength = checksumInput.getMapOutputFileInfo().getMapOutputSize(); // map output size
      long dataLength = checksumInput.getMapOutputFileInfo().getFileBytesWritten(); // bytes written in file
      boolean shuffleInMemory = ramManager.canFitInMemory(mapOutputLength);
      // Shuffle
      if (shuffleInMemory) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Shuffling " + mapOutputLength + " bytes (" +
                   dataLength + " raw bytes) " +
                   "into RAM from " + mapOutputLoc.getTaskAttemptId());
        }
        mapOutput = shuffleInMemory(mapOutputLoc, null, (InputStream) input,
                                    (int)mapOutputLength,
                                    (int)dataLength,
                                    false);
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("Shuffling " + mapOutputLength + " bytes (" +
                   fileSize + " raw bytes) " + "into Local-FS from " +
                   mapOutputLoc.getTaskAttemptId());
        }
        mapOutput = shuffleToDisk(mapOutputLoc, (InputStream) input, filename,
                                  fileSize, false);
      }
      // Try to eliminate additional RPC by not doing advise
/*          try {
        input.adviseFile(FadviseType.FILE_DONTNEED, 0 , fileSize);
      } catch (IOException ioe) {
        if (LOG.isInfoEnabled())
          LOG.info("Error " + ioe + " in fadvise. Ignoring it.");
      }
*/
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
    } catch (IOExceptionWithErrorCode ioe) {
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
    } catch (IOException ioe) {
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

  /**
   * Get the map output into a local file (either in the inmemory fs or on the 
   * local fs) from the remote server.
   * We use the file system so that we generate checksum files on the data.
   * @param mapOutputLoc map-output to be fetched
   * @param filename the filename to write the data into
   * @param connectionTimeout number of milliseconds for connection timeout
   * @param readTimeout number of milliseconds for read timeout
   * @return the path of the file that got created
   * @throws IOException when something goes wrong
   */
  private MapOutput getMapOutput(MapOutputLocation mapOutputLoc,
                                 Path filename, int reduce)
  throws IOException, InterruptedException {
    // Connect
    URL url = mapOutputLoc.getOutputLocation();
    URLConnection connection = url.openConnection();

    InputStream input = setupSecureConnection(mapOutputLoc, connection);

    // Validate header from map output
    TaskAttemptID mapId = null;
    try {
      mapId =
        TaskAttemptID.forName(connection.getHeaderField(FROM_MAP_TASK));
    } catch (IllegalArgumentException ia) {
      LOG.warn("Invalid map id ", ia);
      return null;
    }
    TaskAttemptID expectedMapId = mapOutputLoc.getTaskAttemptId();
    if (!mapId.equals(expectedMapId)) {
      LOG.warn("data from wrong map:" + mapId +
          " arrived to reduce task " + reduce +
          ", where as expected map output should be from " + expectedMapId);
      return null;
    }

    long decompressedLength =
      Long.parseLong(connection.getHeaderField(RAW_MAP_OUTPUT_LENGTH));
    long compressedLength =
      Long.parseLong(connection.getHeaderField(MAP_OUTPUT_LENGTH));

    if (compressedLength < 0 || decompressedLength < 0) {
      LOG.warn(getName() + " invalid lengths in map output header: id: " +
          mapId + " compressed len: " + compressedLength +
          ", decompressed len: " + decompressedLength);
      return null;
    }
    int forReduce =
        (int)Integer.parseInt(connection.getHeaderField(FOR_REDUCE_TASK));

      if (forReduce != reduce) {
        LOG.warn("data for the wrong reduce: " + forReduce +
            " with compressed len: " + compressedLength +
            ", decompressed len: " + decompressedLength +
            " arrived to reduce task " + reduce);
        return null;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("header: " + mapId + ", compressed len: " + compressedLength +
            ", decompressed len: " + decompressedLength);
      }

      //We will put a file in memory if it meets certain criteria:
      //1. The size of the (decompressed) file should be less than 25% of 
      //    the total inmem fs
      //2. There is space available in the inmem fs

      // Check if this map-output can be saved in-memory
      boolean shuffleInMemory = ramManager.canFitInMemory(decompressedLength);

      // Shuffle
      MapOutput mapOutput = null;
      if (shuffleInMemory) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Shuffling " + decompressedLength + " bytes (" +
              compressedLength + " raw bytes) " +
              "into RAM from " + mapOutputLoc.getTaskAttemptId());
        }

        mapOutput = shuffleInMemory(mapOutputLoc, connection, input,
                                    (int)decompressedLength,
                                    (int)compressedLength, true);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Shuffling " + decompressedLength + " bytes (" +
              compressedLength + " raw bytes) " +
              "into Local-FS from " + mapOutputLoc.getTaskAttemptId());
        }

        mapOutput = shuffleToDisk(mapOutputLoc, input, filename,
            compressedLength, true);
      }

      return mapOutput;
    }

  private InputStream setupSecureConnection(MapOutputLocation mapOutputLoc,
      URLConnection connection) throws IOException {

    // generate hash of the url
    String msgToEncode =
      SecureShuffleUtils.buildMsgFrom(connection.getURL());
    String encHash = SecureShuffleUtils.hashFromString(msgToEncode,
        jobTokenSecret);

    // put url hash into http header
    connection.setRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);

    InputStream input = getInputStream(connection, shuffleConnectionTimeout,
                                       shuffleReadTimeout);

    // get the replyHash which is HMac of the encHash we sent to the server
    String replyHash = connection.getHeaderField(
        SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
    if(replyHash==null) {
      throw new IOException("security validation of TT Map output failed");
    }
    if (LOG.isDebugEnabled())
      LOG.debug("url="+msgToEncode+";encHash="+encHash+";replyHash="
          +replyHash);
    // verify that replyHash is HMac of encHash
    SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecret);
    if (LOG.isDebugEnabled())
      LOG.debug("for url="+msgToEncode+" sent hash and receievd reply");
    return input;
  }
  /** 
   * The connection establishment is attempted multiple times and is given up 
   * only on the last failure. Instead of connecting with a timeout of 
   * X, we try connecting with a timeout of x < X but multiple times. 
   */
  private InputStream getInputStream(URLConnection connection,
                                     int connectionTimeout,
                                     int readTimeout)
  throws IOException {
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout "
                            + "[timeout = " + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout)
             ? connectionTimeout
             : UNIT_CONNECT_TIMEOUT;
    }
    // set the read timeout to the total timeout
    connection.setReadTimeout(readTimeout);
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    while (true) {
      try {
        connection.connect();
        break;
      } catch (IOException ioe) {
        // update the total remaining connect-timeout
        connectionTimeout -= unit;

        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (connectionTimeout == 0) {
          throw ioe;
        }

        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
      }
    }
    try {
      return connection.getInputStream();
    } catch (IOException ioe) {
      readError = true;
      throw ioe;
    }
  }

  private MapOutput shuffleInMemory(MapOutputLocation mapOutputLoc,
                                    URLConnection connection,
                                    InputStream input,
                                    int mapOutputLength,
                                    int compressedLength,
                                    boolean shouldCloseInput)
  throws IOException, InterruptedException {
    if (connection != null) {
      // Reserve ram for the map-output
      boolean createdNow = ramManager.reserve(mapOutputLength, input);
      // Reconnect if we need to
      if (!createdNow) {
        // Reconnect
        try {
          connection = mapOutputLoc.getOutputLocation().openConnection();
          input = setupSecureConnection(mapOutputLoc, connection);
          // AH         
          // input = getInputStream(connection, STALLED_COPY_TIMEOUT, 
          //                       DEFAULT_READ_TIMEOUT);
        } catch (IOException ioe) {
          LOG.info("Failed reopen connection to fetch map-output from " +
                   mapOutputLoc.getHost());
          // Inform the ram-manager
          ramManager.closeInMemoryFile(mapOutputLength);
          ramManager.unreserve(mapOutputLength);
          throw ioe;
        }
      }
      IFileInputStream checksumIn = new IFileInputStream(input,compressedLength);
      input = checksumIn;
    } else {
      // Reserve ram for the map-output
      ramManager.reserve(mapOutputLength, null);
    }

    // Are map-outputs compressed?
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }

    // Copy map-output into an in-memory buffer
    byte[] shuffleData = new byte[mapOutputLength];
    MapOutput mapOutput =
      new MapOutput(mapOutputLoc.getTaskId(),
                    mapOutputLoc.getTaskAttemptId(), shuffleData,
                    compressedLength);
    int bytesRead = 0;
    try {
      int n = IOUtils.wrappedReadForCompressedData(LOG, input, shuffleData, 0,
          shuffleData.length);
      while (n > 0) {
        bytesRead += n;
        shuffleClientMetrics.inputBytes(n);

        // indicate we're making progress
        reporter.progress();
        n = IOUtils.wrappedReadForCompressedData(LOG, input, shuffleData,
            bytesRead, shuffleData.length - bytesRead);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Read " + bytesRead + " bytes from map-output for " +
            mapOutputLoc.getTaskAttemptId());
      }

      if (shouldCloseInput) {
        input.close();
      }
    } catch (IOException ioe) {
      LOG.info("Failed to shuffle from " + mapOutputLoc.getTaskAttemptId(),
               ioe);
      // Inform the ram-manager
      ramManager.closeInMemoryFile(mapOutputLength);
      ramManager.unreserve(mapOutputLength);

      // Discard the map-output
      try {
        mapOutput.discard();
      } catch (IOException ignored) {
        LOG.info("Failed to discard map-output from " +
                 mapOutputLoc.getTaskAttemptId(), ignored);
      }
      mapOutput = null;

      // Close the streams
      if (shouldCloseInput) {
        IOUtils.cleanup(LOG, input);
      }

      // Re-throw
      readError = true;
      throw ioe;
    }

    // Close the in-memory file
    ramManager.closeInMemoryFile(mapOutputLength);

    // Sanity check
    if (bytesRead != mapOutputLength) {
      // Inform the ram-manager
      ramManager.unreserve(mapOutputLength);

      // Discard the map-output
      try {
        mapOutput.discard();
      } catch (IOException ignored) {
        // IGNORED because we are cleaning up
        LOG.info("Failed to discard map-output from " +
                 mapOutputLoc.getTaskAttemptId(), ignored);
      }
      mapOutput = null;

      throw new IOException("Incomplete map output received for " +
                            mapOutputLoc.getTaskAttemptId() + " from " +
                            mapOutputLoc.shuffleRootFid.fid + " (" +
                            bytesRead + " instead of " +
                            mapOutputLength + ")"
      );
    }

    // TODO: Remove this after a 'fix' for HADOOP-3647
    if (LOG.isDebugEnabled()) {
      if (mapOutputLength > 0) {
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(shuffleData, 0, shuffleData.length);
        LOG.debug("Rec #1 from " + mapOutputLoc.getTaskAttemptId() +
            " -> (" + WritableUtils.readVInt(dib) + ", " +
            WritableUtils.readVInt(dib) + ") from " +
            mapOutputLoc.getHost());
      }
    }

    return mapOutput;
  }
  
  private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
      InputStream input,
      Path filename,
      long mapOutputLength,
      boolean shouldCloseInput)
          throws IOException {
    // Find out a suitable location for the output on local-filesystem
    Path localFilename = (useMapRFs) ? filename :
    lDirAlloc.getLocalPathForWrite(filename.toUri().getPath(),
    mapOutputLength, conf);
    MapOutput mapOutput =
    new MapOutput(mapOutputLoc.getTaskId(), mapOutputLoc.getTaskAttemptId(),
    conf, localFileSys.makeQualified(localFilename),
    mapOutputLength);
    
    // Copy data to local-disk
    OutputStream output = null;
    long bytesRead = 0;
    try {
    output = rfs.create(localFilename);
    
    byte[] buf = new byte[64 * 1024];
    int n = -1;
    try {
       n = input.read(buf, 0, buf.length);
    } catch (IOException ioe) {
      readError = true;
      throw ioe;
    }
    while (n > 0) {
      bytesRead += n;
      shuffleClientMetrics.inputBytes(n);
      output.write(buf, 0, n);
      
      // indicate we're making progress
      reporter.progress();
      try {
        n = input.read(buf, 0, buf.length);
      } catch (IOException ioe) {
        readError = true;
        throw ioe;
      }
    }

    LOG.info("Read " + bytesRead + " bytes from map-output for " +
        mapOutputLoc.getTaskAttemptId());

    output.close();

    if (shouldCloseInput) {
      input.close();
    }
  } catch (IOException ioe) {
    LOG.info("Failed to shuffle from " + mapOutputLoc.getTaskAttemptId(),
             ioe);

    // Discard the map-output
    try {
      mapOutput.discard();
    } catch (IOException ignored) {
      LOG.info("Failed to discard map-output from " +
          mapOutputLoc.getTaskAttemptId(), ignored);
    }
    mapOutput = null;

    if (shouldCloseInput) {
      // Close the streams
      IOUtils.cleanup(LOG, input, output);
    } else {
      IOUtils.cleanup(LOG, output);
    }
    // Re-throw
    throw ioe;
  }

  // Sanity check
  if (bytesRead != mapOutputLength) {
    try {
      mapOutput.discard();
    } catch (Exception ioe) {
      // IGNORED because we are cleaning up
      LOG.info("Failed to discard map-output from " +
          mapOutputLoc.getTaskAttemptId(), ioe);
    } catch (Throwable t) {
      String msg = getTaskID() + " : Failed in shuffle to disk :"
                   + StringUtils.stringifyException(t);
      reportFatalError(getTaskID(), t, msg);
    }
    mapOutput = null;

    throw new IOException("Incomplete map output received for " +
                          mapOutputLoc.getTaskAttemptId() + " from " +
                          mapOutputLoc.shuffleRootFid.fid + " (" +
                          bytesRead + " instead of " +
                          mapOutputLength + ")"
    );
  }

  return mapOutput;
  }
}
