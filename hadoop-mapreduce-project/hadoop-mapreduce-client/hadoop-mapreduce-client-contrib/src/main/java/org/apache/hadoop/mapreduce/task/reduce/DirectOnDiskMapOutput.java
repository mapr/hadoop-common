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
  private final DirectShuffleMergeManagerImpl<K, V> merger;
  private final OutputStream disk; 
  private long compressedSize;
  private boolean shouldCloseInput; // in case of MapR it is always false

  public DirectOnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
      DirectShuffleMergeManagerImpl<K,V> merger, long size,
      JobConf conf,
      MapOutputFile mapOutputFile, // MapRFSOutputFile
      int fetcher, boolean primaryMapOutput) throws IOException {
    this(mapId, reduceId, merger, size, conf, mapOutputFile, fetcher,
        primaryMapOutput, FileSystem.get(conf), // not a local FS
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  @VisibleForTesting
  DirectOnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         DirectShuffleMergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    super(mapId, size, primaryMapOutput);
    this.fs = fs;
    this.merger = merger;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    disk = fs.create(tmpOutputPath);
  }

  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    return outPath.suffix(String.valueOf(fetcher));
  }

  @Override
  public void shuffle(MapHost host, InputStream input, long mapOutputLength,
      long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter)
      throws IOException {
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
  if (bytesRead != mapOutputLength) {
    throw new IOException("Incomplete map output received for " +
                          getMapId() + " from " +
                          host.getHostName() + " (" +
                          bytesRead + " instead of " +
                          mapOutputLength + ")"
    );
  }

  }

  @Override
  public void commit() throws IOException {
    if (!fs.rename(tmpOutputPath, outputPath)) {
      fs.delete(tmpOutputPath, false);
      throw new IOException("Failed to rename map output " +
      		tmpOutputPath + " to " + outputPath);
    }
    merger.closeOnDiskFile(fs.getFileStatus(outputPath)); 
  }

  @Override
  public void abort() {
    try {
      fs.delete(tmpOutputPath, false);
    } catch (IOException e) {
      LOG.info("failure to clean up " + tmpOutputPath, e);
    }
    
  }

  @Override
  public String getDescription() {
    return "DISK";
  }

}
