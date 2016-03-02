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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombineValuesIterator;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapOutput.MapOutputComparator;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings(value={"unchecked"})
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class DirectShuffleMergeManagerImpl<K, V> implements MergeManager<K, V> {
  
  private static final Log LOG = LogFactory.getLog(DirectShuffleMergeManagerImpl.class);
  
  /* Maximum percentage of the in-memory limit that a single shuffle can 
   * consume*/ 
  private static final float DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT
    = 0.25f;

  private final TaskAttemptID reduceId;
  
  private final JobConf jobConf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  
  protected MapOutputFile mapOutputFile;
  
  Set<DirectInMemoryOutput<K, V>> inMemoryMergedMapOutputs = 
    new TreeSet<DirectInMemoryOutput<K,V>>(new MapOutputComparator<K, V>());
  private IntermediateMemoryToMemoryMerger memToMemMerger;

  Set<DirectInMemoryOutput<K, V>> inMemoryMapOutputs = 
    new TreeSet<DirectInMemoryOutput<K,V>>(new MapOutputComparator<K, V>());
  private final MergeThread<DirectInMemoryOutput<K,V>, K,V> inMemoryMerger;
  
  Set<FileStatus> onDiskMapOutputs = new TreeSet<FileStatus>(new Comparator<FileStatus>(){

    @Override
    public int compare(FileStatus o1, FileStatus o2) {
      int diff = Long.signum(o1.getLen() - o2.getLen());
      if ( diff != 0 ) {
        return diff;
      }
      return o1.compareTo(o2);
    }});
  
  private final OnDiskMerger onDiskMerger;
  
  private final long memoryLimit;
  private long usedMemory;
  private long commitMemory;
  private final long maxSingleShuffleLimit;
  
  private final int memToMemMergeOutputsThreshold; 
  private final long mergeThreshold;
  
  private final int ioSortFactor;

  private final Reporter reporter;
  private final ExceptionReporter exceptionReporter;
  
  /**
   * Combiner class to run during in-memory merge, if defined.
   */
  private final Class<? extends Reducer> combinerClass;

  /**
   * Resettable collector used for combine.
   */
  private final CombineOutputCollector<K,V> combineCollector;

  private final Counters.Counter spilledRecordsCounter;

  private final Counters.Counter reduceCombineInputCounter;

  private final Counters.Counter mergedMapOutputsCounter;
  
  private final CompressionCodec codec;
  
  private final Progress mergePhase;

  public DirectShuffleMergeManagerImpl(TaskAttemptID reduceId, JobConf jobConf, 
                      FileSystem localFS,
                      Reporter reporter,
                      CompressionCodec codec,
                      Class<? extends Reducer> combinerClass,
                      CombineOutputCollector<K,V> combineCollector,
                      Counters.Counter spilledRecordsCounter,
                      Counters.Counter reduceCombineInputCounter,
                      Counters.Counter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      Progress mergePhase, MapOutputFile mapOutputFile) throws IOException {
    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.exceptionReporter = exceptionReporter;
    
    this.reporter = reporter;
    this.codec = codec;
    this.combinerClass = combinerClass;
    this.combineCollector = combineCollector;
    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = mapOutputFile;
    this.mapOutputFile.setConf(jobConf);
    
    this.localFS = FileSystem.get(jobConf);
    this.rfs = this.localFS;
    
    final float maxInMemCopyUse =
      jobConf.getFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 0.90f);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    this.memoryLimit = (long)(jobConf.getLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
          Runtime.getRuntime().maxMemory()) * maxInMemCopyUse);
 
    this.ioSortFactor = jobConf.getInt(MRJobConfig.IO_SORT_FACTOR, 100);

    final float singleShuffleMemoryLimitPercent =
        jobConf.getFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT,
            DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    usedMemory = 0L;
    commitMemory = 0L;
    this.maxSingleShuffleLimit = 
      (long)(memoryLimit * singleShuffleMemoryLimitPercent);
    this.memToMemMergeOutputsThreshold = 
            jobConf.getInt(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, ioSortFactor);
    this.mergeThreshold = (long)(this.memoryLimit * 
                          jobConf.getFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 
                                           0.90f));
    LOG.info("MergerManager: memoryLimit=" + memoryLimit + ", " +
             "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
             "mergeThreshold=" + mergeThreshold + ", " + 
             "ioSortFactor=" + ioSortFactor + ", " +
             "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);

    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invlaid configuration: "
          + "maxSingleShuffleLimit should be less than mergeThreshold"
          + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
          + "mergeThreshold: " + this.mergeThreshold);
    }

    boolean allowMemToMemMerge = 
      jobConf.getBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, false);
    if (allowMemToMemMerge) {
      this.memToMemMerger = 
        new IntermediateMemoryToMemoryMerger(this,
                                             memToMemMergeOutputsThreshold);
      this.memToMemMerger.start();
    } else {
      this.memToMemMerger = null;
    }
    
    this.inMemoryMerger = createInMemoryMerger();
    this.inMemoryMerger.start();
    
    this.onDiskMerger = new OnDiskMerger(this);
    this.onDiskMerger.start();
    
    this.mergePhase = mergePhase;
  }
  
  protected MergeThread<DirectInMemoryOutput<K,V>, K,V> createInMemoryMerger() {
    return new InMemoryMerger(this);
  }

  TaskAttemptID getReduceId() {
    return reduceId;
  }

  @VisibleForTesting
  ExceptionReporter getExceptionReporter() {
    return exceptionReporter;
  }

  @Override
  public void waitForResource() throws InterruptedException {
    inMemoryMerger.waitForMerge();
  }
  
  @VisibleForTesting
  protected boolean canShuffleToMemory(long requestedSize) {
    return (requestedSize < maxSingleShuffleLimit); 
  }
  
  @Override
  public synchronized MapOutput<K,V> reserve(TaskAttemptID mapId, 
                                             long requestedSize,
                                             int fetcher
                                             ) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info(mapId + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return new DirectOnDiskMapOutput<K,V>(mapId, reduceId, this, requestedSize,
                                      jobConf, mapOutputFile, fetcher, true);
    }
    
    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above limit &&
    // requested size < singleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check (usedMemory > memoryLimit) and not
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads
    
    while (usedMemory > memoryLimit) {
      LOG.debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory
          + ") is greater than memoryLimit (" + memoryLimit + ")." + 
          " CommitMemory is (" + commitMemory + ")");
      try {
        LOG.info("fetcher#" + fetcher + " - MergeManager returned status WAIT ...");
        this.wait(); // block until memory is available
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    
    // Allow the in-memory shuffle to progress
    LOG.debug(mapId + ": Proceeding with shuffle since usedMemory ("
        + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
        + "CommitMemory is (" + commitMemory + ")"); 
    return unconditionalReserve(mapId, requestedSize, true);
  }
  
  /**
   * Unconditional Reserve is used by the Memory-to-Memory thread
   * @return
   */
  @VisibleForTesting
  protected synchronized DirectInMemoryOutput<K, V> unconditionalReserve(
      TaskAttemptID mapId, long requestedSize, boolean primaryMapOutput) {
    usedMemory += requestedSize;
    return new DirectInMemoryOutput<K,V>(jobConf, mapId, this, (int)requestedSize,
                                      codec, primaryMapOutput);
  }
  
  synchronized void unreserve(long size) {
    usedMemory -= size;
    this.notifyAll();// notify the waiting fetchers
  }

  public synchronized void closeInMemoryFile(DirectInMemoryOutput<K,V> mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
        + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
        + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory+= mapOutput.getSize();

    // Can hang if mergeThreshold is really low.
    if (commitMemory >= mergeThreshold) {
      LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
          commitMemory + " > mergeThreshold=" + mergeThreshold + 
          ". Current usedMemory=" + usedMemory);
      inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
      inMemoryMergedMapOutputs.clear();
      inMemoryMerger.startMerge(inMemoryMapOutputs);
      commitMemory = 0L;  // Reset commitMemory.
    }
    
    if (memToMemMerger != null) {
      if (inMemoryMapOutputs.size() >= memToMemMergeOutputsThreshold) { 
        memToMemMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }
  
  
  public synchronized void closeInMemoryMergedFile(DirectInMemoryOutput<K,V> mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryMergedFile -> size: " + mapOutput.getSize() + 
             ", inMemoryMergedMapOutputs.size() -> " + 
             inMemoryMergedMapOutputs.size());
  }
  
  public synchronized void closeOnDiskFile(FileStatus fileStatus) {
    onDiskMapOutputs.add(fileStatus);
    
    if (onDiskMapOutputs.size() >= (2 * ioSortFactor - 1)) {
      onDiskMerger.startMerge(onDiskMapOutputs);
    }
}

  
  @Override
  public RawKeyValueIterator close() throws Throwable {
    // Wait for on-going merges to complete
    if (memToMemMerger != null) { 
      memToMemMerger.close();
    }
    inMemoryMerger.close();
    onDiskMerger.close();
    
    List<DirectInMemoryOutput<K, V>> memory = 
      new ArrayList<DirectInMemoryOutput<K, V>>(inMemoryMergedMapOutputs);
    inMemoryMergedMapOutputs.clear();
    memory.addAll(inMemoryMapOutputs);
    inMemoryMapOutputs.clear();
    List<FileStatus> disk = new ArrayList<FileStatus>(onDiskMapOutputs);
    onDiskMapOutputs.clear();
    return finalMerge(jobConf, rfs, memory, disk);
  }
   
  private class IntermediateMemoryToMemoryMerger 
  extends MergeThread<DirectInMemoryOutput<K, V>, K, V> {
    
    public IntermediateMemoryToMemoryMerger(DirectShuffleMergeManagerImpl<K, V> manager, 
                                            int mergeFactor) {
      super(manager, mergeFactor, exceptionReporter);
      setName("InMemoryMerger - Thread to do in-memory merge of in-memory " +
              "shuffled map-outputs");
      setDaemon(true);
    }

    @Override
    public void merge(List<DirectInMemoryOutput<K, V>> inputs) throws IOException {
      if (inputs == null || inputs.size() == 0) {
        return;
      }

      TaskAttemptID dummyMapId = inputs.get(0).getMapId(); 
      List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
      long mergeOutputSize = 
        createInMemorySegments(inputs, inMemorySegments, 0);
      int noInMemorySegments = inMemorySegments.size();
      
      DirectInMemoryOutput<K, V> mergedMapOutputs = 
        unconditionalReserve(dummyMapId, mergeOutputSize, false);
      
      Writer<K, V> writer = 
        new InMemoryWriter<K, V>(mergedMapOutputs.getArrayStream());
      
      LOG.info("Initiating Memory-to-Memory merge with " + noInMemorySegments +
               " segments of total-size: " + mergeOutputSize);

      RawKeyValueIterator rIter = 
        Merger.merge(jobConf, rfs,
                     (Class<K>)jobConf.getMapOutputKeyClass(),
                     (Class<V>)jobConf.getMapOutputValueClass(),
                     inMemorySegments, inMemorySegments.size(),
                     new Path(reduceId.toString()),
                     (RawComparator<K>)jobConf.getOutputKeyComparator(),
                     reporter, null, null, null);
      Merger.writeFile(rIter, writer, reporter, jobConf);
      writer.close();

      LOG.info(reduceId +  
               " Memory-to-Memory merge of the " + noInMemorySegments +
               " files in-memory complete.");

      // Note the output of the merge
      closeInMemoryMergedFile(mergedMapOutputs);
    }
  }
  
  private class InMemoryMerger extends MergeThread<DirectInMemoryOutput<K,V>, K,V> {
    
    public InMemoryMerger(DirectShuffleMergeManagerImpl<K, V> manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName
      ("InMemoryMerger - Thread to merge in-memory shuffled map-outputs");
      setDaemon(true);
    }
    
    @Override
    public void merge(List<DirectInMemoryOutput<K,V>> inputs) throws IOException {
      if (inputs == null || inputs.size() == 0) {
        return;
      }
      
      //name this output file same as the name of the first file that is 
      //there in the current list of inmem files (this is guaranteed to
      //be absent on the disk currently. So we don't overwrite a prev. 
      //created spill). Also we need to create the output file now since
      //it is not guaranteed that this file will be present after merge
      //is called (we delete empty files as soon as we see them
      //in the merge method)

      //figure out the mapId 
      TaskAttemptID mapId = inputs.get(0).getMapId();
      TaskID mapTaskId = mapId.getTaskID();

      List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
      long mergeOutputSize = 
        createInMemorySegments(inputs, inMemorySegments,0);
      int noInMemorySegments = inMemorySegments.size();

      // TODO - no suffix for mem files???
      Path outputPath = 
        mapOutputFile.getInputFileForWrite(mapTaskId,
                                           mergeOutputSize); //.suffix(
                                               //Task.MERGED_OUTPUT_PREFIX);

      Writer<K,V> writer = 
        new Writer<K,V>(jobConf, rfs.create(outputPath),
                        (Class<K>) jobConf.getMapOutputKeyClass(),
                        (Class<V>) jobConf.getMapOutputValueClass(), codec, null, true);

      RawKeyValueIterator rIter = null;
      FileStatus fileStatus = null;
      try {
        LOG.info("Initiating in-memory merge with " + noInMemorySegments + 
                 " segments...");
        
        rIter = Merger.merge(jobConf, rfs,
                             (Class<K>)jobConf.getMapOutputKeyClass(),
                             (Class<V>)jobConf.getMapOutputValueClass(),
                             inMemorySegments, inMemorySegments.size(),
                             new Path(reduceId.toString()),
                             (RawComparator<K>)jobConf.getOutputKeyComparator(),
                             reporter, spilledRecordsCounter, null, null);
        
        if (null == combinerClass) {
          Merger.writeFile(rIter, writer, reporter, jobConf);
        } else {
          combineCollector.setWriter(writer);
          combineAndSpill(rIter, reduceCombineInputCounter);
        }
        writer.close();
        fileStatus = localFS.getFileStatus(outputPath);

        LOG.info(reduceId +  
            " Merge of the " + noInMemorySegments +
            " files in-memory complete." +
            " file is " + outputPath + " of size " + 
            localFS.getFileStatus(outputPath).getLen());
      } catch (IOException e) { 
        //make sure that we delete the ondisk file that we created 
        //earlier when we invoked cloneFileAttributes
        localFS.delete(outputPath, true);
        throw e;
      }

      // Note the output of the merge
      closeOnDiskFile(fileStatus);
    }

  }
  
  private class OnDiskMerger extends MergeThread<FileStatus,K,V> {
    
    public OnDiskMerger(DirectShuffleMergeManagerImpl<K, V> manager) {
      super(manager, ioSortFactor, exceptionReporter);
      setName("OnDiskMerger - Thread to merge on-disk map-outputs");
      setDaemon(true);
    }
    
    @Override
    public void merge(List<FileStatus> inputs) throws IOException {
      // sanity check
      if (inputs == null || inputs.isEmpty()) {
        LOG.info("No ondisk files to merge...");
        return;
      }
      
      long approxOutputSize = 0;
      
      LOG.info("OnDiskMerger: We have  " + inputs.size() + 
               " map outputs on disk. Triggering merge...");
      
      // 1. Prepare the list of files to be merged. 
      Path[] paths = new Path[inputs.size()];
      for (int i = 0; i < inputs.size(); i++) {
        approxOutputSize += inputs.get(i).getLen();
        paths[i] = inputs.get(i).getPath();
      }

      // for MapR do not add Checksum len
      // 2. Start the on-disk merge process
      Path outputPath = ((MapRFsOutputFile) mapOutputFile).getLocalPathForWrite(
          paths[0].toString(),
          approxOutputSize).suffix(Task.MERGED_OUTPUT_PREFIX);

      Writer<K,V> writer = 
        new Writer<K,V>(jobConf, rfs.create(outputPath), 
                        (Class<K>) jobConf.getMapOutputKeyClass(), 
                        (Class<V>) jobConf.getMapOutputValueClass(), codec, null, true);
      RawKeyValueIterator iter  = null;
      FileStatus fileStatus;
      Path tmpDir = ((MapRFsOutputFile)mapOutputFile).getLocalPathForWrite(
          reduceId.getTaskID().toString(), -1);
      try {
        iter = Merger.merge(jobConf, rfs,
                            (Class<K>) jobConf.getMapOutputKeyClass(),
                            (Class<V>) jobConf.getMapOutputValueClass(),
                            codec, paths,
                            true, ioSortFactor, tmpDir, 
                            (RawComparator<K>) jobConf.getOutputKeyComparator(), 
                            reporter, spilledRecordsCounter, null, 
                            mergedMapOutputsCounter, null);

        Merger.writeFile(iter, writer, reporter, jobConf);
        writer.close();
        fileStatus = rfs.getFileStatus(outputPath);
      } catch (IOException e) {
        localFS.delete(outputPath, true);
        throw e;
      }

      closeOnDiskFile(fileStatus);

      LOG.info(reduceId +
          " Finished merging " + inputs.size() + 
          " map output files on disk of total-size " + 
          approxOutputSize + "." + 
          " Local output file is " + outputPath + " of size " +
          localFS.getFileStatus(outputPath).getLen());
    }
  }
  
  private void combineAndSpill(
      RawKeyValueIterator kvIter,
      Counters.Counter inCounter) throws IOException {
    JobConf job = jobConf;
    Reducer combiner = ReflectionUtils.newInstance(combinerClass, job);
    Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
    Class<V> valClass = (Class<V>) job.getMapOutputValueClass();
    RawComparator<K> comparator = 
      (RawComparator<K>)job.getOutputKeyComparator();
    try {
      CombineValuesIterator values = new CombineValuesIterator(
          kvIter, comparator, keyClass, valClass, job, Reporter.NULL,
          inCounter);
      while (values.more()) {
        combiner.reduce(values.getKey(), values, combineCollector,
                        Reporter.NULL);
        values.nextKey();
      }
    } finally {
      combiner.close();
    }
  }

  private long createInMemorySegments(List<DirectInMemoryOutput<K,V>> inMemoryMapOutputs,
                                      List<Segment<K, V>> inMemorySegments, 
                                      long leaveBytes
                                      ) throws IOException {
    long totalSize = 0L;
    // We could use fullSize could come from the RamManager, but files can be
    // closed but not yet present in inMemoryMapOutputs
    long fullSize = 0L;
    for (DirectInMemoryOutput<K,V> mo : inMemoryMapOutputs) {
      fullSize += mo.getMemory().length;
    }
    while(fullSize > leaveBytes) {
      DirectInMemoryOutput<K,V> mo = inMemoryMapOutputs.remove(0);
      byte[] data = mo.getMemory();
      long size = data.length;
      totalSize += size;
      fullSize -= size;
      Reader<K,V> reader = new DirectInMemoryReader<K,V>(DirectShuffleMergeManagerImpl.this, 
                                                   mo.getMapId(),
                                                   data, 0, (int)size, jobConf);
      inMemorySegments.add(new Segment<K,V>(reader, true, 
                                            (mo.isPrimaryMapOutput() ? 
                                            mergedMapOutputsCounter : null)));
    }
    return totalSize;
  }

  class RawKVIteratorReader extends IFile.Reader<K,V> {

    private final RawKeyValueIterator kvIter;

    public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
        throws IOException {
      super(null, null, size, null, spilledRecordsCounter);
      this.kvIter = kvIter;
    }
    public boolean nextRawKey(DataInputBuffer key) throws IOException {
      if (kvIter.next()) {
        final DataInputBuffer kb = kvIter.getKey();
        final int kp = kb.getPosition();
        final int klen = kb.getLength() - kp;
        key.reset(kb.getData(), kp, klen);
        bytesRead += klen;
        return true;
      }
      return false;
    }
    public void nextRawValue(DataInputBuffer value) throws IOException {
      final DataInputBuffer vb = kvIter.getValue();
      final int vp = vb.getPosition();
      final int vlen = vb.getLength() - vp;
      value.reset(vb.getData(), vp, vlen);
      bytesRead += vlen;
    }
    public long getPosition() throws IOException {
      return bytesRead;
    }

    public void close() throws IOException {
      kvIter.close();
    }
  }

  private RawKeyValueIterator finalMerge(JobConf job, FileSystem fs,
                                       List<DirectInMemoryOutput<K,V>> inMemoryMapOutputs,
                                       List<FileStatus> onDiskMapOutputs
                                       ) throws IOException {
    LOG.info("finalMerge called with " + 
             inMemoryMapOutputs.size() + " in-memory map-outputs and " + 
             onDiskMapOutputs.size() + " on-disk map-outputs");
    
    final float maxRedPer =
      job.getFloat(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, 0f);
    if (maxRedPer > 1.0 || maxRedPer < 0.0) {
      throw new IOException(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT +
                            maxRedPer);
    }
    long maxInMemReduce = (long)(memoryLimit * maxRedPer);

    // merge config params
    Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
    Class<V> valueClass = (Class<V>)job.getMapOutputValueClass();
    boolean keepInputs = job.getKeepFailedTaskFiles();
    final Path tmpDir = ((MapRFsOutputFile)mapOutputFile).getLocalPathForWrite(
        reduceId.getTaskID().toString(), -1);
    final RawComparator<K> comparator =
      (RawComparator<K>)job.getOutputKeyComparator();

    // segments required to vacate memory
    List<Segment<K,V>> memDiskSegments = new ArrayList<Segment<K,V>>();
    long inMemToDiskBytes = 0;
    boolean mergePhaseFinished = false;
    if (inMemoryMapOutputs.size() > 0) {
      TaskID mapId = inMemoryMapOutputs.get(0).getMapId().getTaskID();
      inMemToDiskBytes = createInMemorySegments(inMemoryMapOutputs, 
                                                memDiskSegments,
                                                maxInMemReduce);
      final int numMemDiskSegments = memDiskSegments.size();
      if (numMemDiskSegments > 0 &&
            ioSortFactor > onDiskMapOutputs.size()) {
        
        // If we reach here, it implies that we have less than io.sort.factor
        // disk segments and this will be incremented by 1 (result of the 
        // memory segments merge). Since this total would still be 
        // <= io.sort.factor, we will not do any more intermediate merges,
        // the merge of all these disk segments would be directly fed to the
        // reduce method
        
        mergePhaseFinished = true;
        // must spill to disk, but can't retain in-mem for intermediate merge
        final Path outputPath = 
          mapOutputFile.getInputFileForWrite(mapId,
                                             inMemToDiskBytes).suffix(
                                                 Task.MERGED_OUTPUT_PREFIX);
        final RawKeyValueIterator rIter = Merger.merge(job, fs,
            keyClass, valueClass, memDiskSegments, numMemDiskSegments,
            tmpDir, comparator, reporter, spilledRecordsCounter, null, 
            mergePhase);

        Writer<K,V> writer = new Writer<K,V>(job, fs.create(outputPath), keyClass, valueClass,
          codec, null, true);
        try {
          Merger.writeFile(rIter, writer, reporter, job);
          writer.close();
          onDiskMapOutputs.add(fs.getFileStatus(outputPath));
          writer = null;
          // add to list of final disk outputs.
        } catch (IOException e) {
          if (null != outputPath) {
            try {
              fs.delete(outputPath, true);
            } catch (IOException ie) {
              // NOTHING
            }
          }
          throw e;
        } finally {
          if (null != writer) {
            writer.close();
          }
        }
        LOG.info("Merged " + numMemDiskSegments + " segments, " +
                 inMemToDiskBytes + " bytes to disk to satisfy " +
                 "reduce memory limit");
        inMemToDiskBytes = 0;
        memDiskSegments.clear();
      } else if (inMemToDiskBytes != 0) {
        LOG.info("Keeping " + numMemDiskSegments + " segments, " +
                 inMemToDiskBytes + " bytes in memory for " +
                 "intermediate, on-disk merge");
      }
    }

    // segments on disk
    List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
    long onDiskBytes = inMemToDiskBytes;
    long rawBytes = inMemToDiskBytes;
    FileStatus[] onDisk = onDiskMapOutputs.toArray(
        new FileStatus[onDiskMapOutputs.size()]);
    for (FileStatus file : onDisk) {
      long fileLength = file.getLen();
      onDiskBytes += fileLength;
     // rawBytes += (file.getRawDataLength() > 0) ? file.getRawDataLength() : fileLength;
      rawBytes += fileLength;

      LOG.debug("Disk file: " + file + " Length is " + fileLength);
      diskSegments.add(new Segment<K, V>(job, fs, file.getPath(), codec, keepInputs,
                                         (file.toString().endsWith(
                                             Task.MERGED_OUTPUT_PREFIX) ?
                                          null : mergedMapOutputsCounter), fileLength /*fileLengthfile.getRawDataLength()*/
                                        ));
    }
    LOG.info("Merging " + onDisk.length + " files, " +
             onDiskBytes + " bytes from disk");
    Collections.sort(diskSegments, new Comparator<Segment<K,V>>() {
      public int compare(Segment<K, V> o1, Segment<K, V> o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }
        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    });

    // build final list of segments from merged backed by disk + in-mem
    List<Segment<K,V>> finalSegments = new ArrayList<Segment<K,V>>();
    long inMemBytes = createInMemorySegments(inMemoryMapOutputs, 
                                             finalSegments, 0);
    LOG.info("Merging " + finalSegments.size() + " segments, " +
             inMemBytes + " bytes from memory into reduce");
    if (0 != onDiskBytes) {
      final int numInMemSegments = memDiskSegments.size();
      diskSegments.addAll(0, memDiskSegments);
      memDiskSegments.clear();
      // Pass mergePhase only if there is a going to be intermediate
      // merges. See comment where mergePhaseFinished is being set
      Progress thisPhase = (mergePhaseFinished) ? null : mergePhase; 
      RawKeyValueIterator diskMerge = Merger.merge(
          job, fs, keyClass, valueClass, codec, diskSegments,
          ioSortFactor, numInMemSegments, tmpDir, comparator,
          reporter, false, spilledRecordsCounter, null, thisPhase);
      diskSegments.clear();
      if (0 == finalSegments.size()) {
        return diskMerge;
      }
      finalSegments.add(new Segment<K,V>(
            new RawKVIteratorReader(diskMerge, onDiskBytes), true, rawBytes));
    }
    return Merger.merge(job, fs, keyClass, valueClass,
                 finalSegments, finalSegments.size(), tmpDir,
                 comparator, reporter, spilledRecordsCounter, null,
                 null); 
  }
}
