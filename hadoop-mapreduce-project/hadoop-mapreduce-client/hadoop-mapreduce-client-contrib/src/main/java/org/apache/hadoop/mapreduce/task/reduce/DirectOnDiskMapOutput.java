package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.google.common.annotations.VisibleForTesting;

public class DirectOnDiskMapOutput<K,V> extends MapOutput<K, V> {

  private static final Log LOG = LogFactory.getLog(DirectOnDiskMapOutput.class);
  private final FileSystem fs;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final MergeManagerImpl<K, V> merger;
  private final OutputStream disk; 
  private long compressedSize;
  private boolean shouldCloseInput; // in case of MapR it is always false

  public DirectOnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
      MergeManagerImpl<K,V> merger, long size,
      JobConf conf,
      MapOutputFile mapOutputFile, // MapRFSOutputFile
      int fetcher, boolean primaryMapOutput) throws IOException {
    this(mapId, reduceId, merger, size, conf, mapOutputFile, fetcher,
        primaryMapOutput, FileSystem.get(conf), // not a local FS
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  @VisibleForTesting
  DirectOnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    super(mapId, size, primaryMapOutput);
    this.fs = fs;
    this.merger = merger;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    disk = fs.create(outputPath);
  }

  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    //TODO ???
    return outPath.suffix(String.valueOf(fetcher));
  }

  @Override
  public void shuffle(MapHost host, InputStream input, long compressedLength,
      long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter)
      throws IOException {
    // TODO we will use decompressed length - but need to double check
    long bytesRead = 0;
    try {
    
    byte[] buf = new byte[64 * 1024];
    int n = -1;
    try {
       n = input.read(buf, 0, buf.length);
    } catch (IOException ioe) {
      //TODO revisit readError
      //readError = true;
      throw ioe;
    }
    while (n > 0) {
      bytesRead += n;
      metrics.inputBytes(n);
      disk.write(buf, 0, n);
      
      // indicate we're making progress
      reporter.progress();
      try {
        n = input.read(buf, 0, buf.length);
      } catch (IOException ioe) {
      //TODO revisit readError
        //readError = true;
        throw ioe;
      }
    }

    LOG.info("Read " + bytesRead + " bytes from map-output for " +
        getMapId());

    disk.close();

    if (shouldCloseInput) {
      input.close();
    }
  } catch (IOException ioe) {
    LOG.info("Failed to shuffle from " + getMapId(),
             ioe);

    // Discard the map-output - caller will need to invoke "abort()" in case of IOException
  /*  try {
      mapOutput.discard();
    } catch (IOException ignored) {
      LOG.info("Failed to discard map-output from " +
          mapOutputLoc.getTaskAttemptId(), ignored);
    }
    mapOutput = null;
*/
    if (shouldCloseInput) {
      // Close the streams
      IOUtils.cleanup(LOG, input, disk);
    } else {
      IOUtils.cleanup(LOG, disk);
    }
    // Re-throw
    throw ioe;
  }

  // Sanity check
  if (bytesRead != decompressedLength) {
    // TODO - caller will need to catch IOException and invoke "abort()" to cleanup
 /*   try {
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
*/
    throw new IOException("Incomplete map output received for " +
                          getMapId() + " from " +
                          //mapOutputLoc.shuffleRootFid + " (" +
                          bytesRead + " instead of " +
                          decompressedLength + ")"
    );
  }

  }

  @Override
  public void commit() throws IOException {
    // TODO
    // merger.closeOnDiskFile(compressAwarePath);  ???
  }

  @Override
  public void abort() {
    try {
      fs.delete(outputPath, true);
    } catch (IOException e) {
      LOG.info("failure to clean up " + outputPath, e);
    }
    
  }

  @Override
  public String getDescription() {
    return "DISK";
  }

}
