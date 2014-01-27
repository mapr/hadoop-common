package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class DirectInMemoryOutput<K,V> extends MapOutput<K, V> {

  private static final Log LOG = LogFactory.getLog(DirectInMemoryOutput.class);
  private Configuration conf;
  private final MergeManagerImpl<K, V> merger;
  private final byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;

  private boolean shouldCloseInput; // always false for MapR
  
  public DirectInMemoryOutput(Configuration conf, TaskAttemptID mapId,
      MergeManagerImpl<K, V> merger,
      int size, CompressionCodec codec,
      boolean primaryMapOutput) {
    super(mapId, (long)size, primaryMapOutput);
    this.conf = conf;
    this.merger = merger;
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      decompressor = null;
    }

  }

  @Override
  public void shuffle(MapHost host, InputStream input, long compressedLength,
      long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter)
      throws IOException {
    
      // Reserve ram for the map-output
    // TODO looks like we don't need to reserve anymore 
      //ramManager.reserve(decompressedLength, null);

  // Are map-outputs compressed?
  if (codec != null) {
    decompressor.reset();
    input = codec.createInputStream(input, decompressor);
  }

  int bytesRead = 0;
  try {
    int n = IOUtils.wrappedReadForCompressedData(input, memory, 0,
        memory.length);
    while (n > 0) {
      bytesRead += n;
      metrics.inputBytes(n);

      // indicate we're making progress
      reporter.progress();
      n = IOUtils.wrappedReadForCompressedData(input, memory,
          bytesRead, memory.length - bytesRead);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Read " + bytesRead + " bytes from map-output for " +
          getMapId());
    }

    if (shouldCloseInput) {
      input.close();
    }
  } catch (IOException ioe) {
    LOG.info("Failed to shuffle from " + getMapId(),
             ioe);
    // Inform the ram-manager
    //ramManager.closeInMemoryFile(decompressedLength);
    //ramManager.unreserve(decompressedLength);

    // Close the streams
    if (shouldCloseInput) {
      IOUtils.cleanup(LOG, input);
    }

    // Re-throw
    // TODO: deal with this readError
    //readError = true;
    throw ioe;
  } finally {
    CodecPool.returnDecompressor(decompressor);
  }

  // Close the in-memory file
  //ramManager.closeInMemoryFile(decompressedLength);

  // Sanity check
  if (bytesRead != decompressedLength) {
    // Inform the ram-manager
    //ramManager.unreserve(decompressedLength);

   
    throw new IOException("Incomplete map output received for " +
        getMapId() + " from " +
        //mapOutputLoc.shuffleRootFid.fid + " (" +
        bytesRead + " instead of " +
        decompressedLength + ")");
  }

  // TODO: Remove this after a 'fix' for HADOOP-3647
  if (LOG.isDebugEnabled()) {
    if (decompressedLength > 0) {
      DataInputBuffer dib = new DataInputBuffer();
      dib.reset(memory, 0, memory.length);
      LOG.debug("Rec #1 from " + getMapId() +
      " -> (" + WritableUtils.readVInt(dib) + ", " +
      WritableUtils.readVInt(dib) + ") from " +
      host.getHostName());
    }
  }
}

  @Override
  public void commit() throws IOException {
    // TODO ???
    //merger.closeInMemoryFile(this);
  }

  @Override
  public void abort() {
    merger.unreserve(memory.length);
  }

  @Override
  public String getDescription() {
    return "MEMORY";
  }

}
