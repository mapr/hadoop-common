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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapred.MapTask.MapBufferTooSmallException;
import org.apache.hadoop.mapred.MapRFsOutputFile;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.HasRawComparablePrefix;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;


public class MapRFsOutputBuffer<K extends Object, V extends Object>
    implements MapOutputCollector<K, V>, IndexedSortable {
    //extends MapOutputBuffer<K,V> {
      
  private static final Log LOG = LogFactory.getLog(MapRFsOutputBuffer.class);
  
  private int partitions;
  private JobConf job;
  private TaskReporter reporter;
  private Class<K> keyClass;
  private Class<V> valClass;
  private RawComparator<K> comparator;
  private SerializationFactory serializationFactory;
  private Serializer<K> keySerializer;
  private Serializer<V> valSerializer;
  private CombinerRunner<K,V> combinerRunner;
  private CombineOutputCollector<K, V> combineCollector;

  // Compression for map-outputs
  private CompressionCodec codec;

  // k/v accounting
  private IntBuffer kvmeta; // metadata overlay on backing store
  volatile int kvstart = 0; // marks origin of spill metadata
  volatile int kvend = 0;   // marks end of spill metadata
  int kvindex = 0;          // marks end of fully serialized records

  int equator;            // marks origin of meta/serialization
  volatile int bufstart = 0;           // marks beginning of spill
  volatile int bufend = 0;             // marks beginning of collectable
  int bufmark = 0;            // marks end of record
  int bufindex = 0;           // marks end of collected
  volatile int bufvoid = 0;            // marks the point where we should stop
                          // reading at the end of the buffer

  byte[] kvbuffer;        // main output buffer
  private final byte[] b0 = new byte[0];

  private static final int VALSTART = 0;         // val offset in acct
  private static final int KEYSTART = 1;         // key offset in acct
  private static final int PARTITION = 2;        // partition offset in acct
  private static final int VALLEN = 3;           // length of value
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  // spill accounting
  private int maxRec;
  private int softLimit;
  private int softRecordLimit;
  boolean spillInProgress;
  int bufferRemaining;
  volatile Throwable sortSpillException = null;

  volatile int numSpills = 0;
  private int minSpillsForCombine;
  private IndexedSorter sorter;
  final ReentrantLock spillLock = new ReentrantLock();
  final Condition spillDone = spillLock.newCondition();
  final Condition spillReady = spillLock.newCondition();
  final BlockingBuffer bb = new BlockingBuffer();
  volatile boolean spillThreadRunning = false;
  final MapRSpillThread spillThread = new MapRSpillThread();


  private FileSystem rfs;

  // Counters
  private Counters.Counter mapOutputByteCounter;
  private Counters.Counter mapOutputRecordCounter;
  private Counters.Counter fileOutputByteCounter;

  final ArrayList<SpillRecord> indexCacheList =
    new ArrayList<SpillRecord>();
  private int totalIndexCacheMemory;
  private int indexCacheMemoryLimit;
  private static final int INDEX_CACHE_MEMORY_LIMIT_DEFAULT = 1024 * 1024;

  private MapTask mapTask;
  private MapRFsOutputFile mapOutputFile;
  private Progress sortPhase;
  private Counters.Counter spilledRecordsCounter;
  
  private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;
  private static final int MIN_KEYPREFIX_INTS = 1;
  private static final int MAX_KEYPREFIX_INTS = 4;
  private static final int KEYPREFIX = 0;  // key offset in acct
  
  private ExecutorService threadPool;
  private long outputSize = 0L;
  private int kvoffsetTotal;
  private KvOffset[] kvoffsets;
  private KvOffset[] oldKvoffsets = null;     // indices into kvindices
  private int[] kvindices;     // partition, k/v offsets into kvbuffer
  private int currentThreadsNumber;
  private int closerThreadsNumber;
  private int keyPrefixLen;    // key prefix length in bytes
  private int keyStart;              // key offset in acct
  private int valStart;              // val offset in acct
  private int acctSize;              // total #fields in acct
  private int recSize;               // acct bytes per record
  private byte[] prefixdata;  
  private TaskAttemptID mapId;
  
  private void initFS(TaskAttemptID mapId) throws IOException {
    if (LOG.isDebugEnabled())
      LOG.debug("Initializing MapRFsOutputBuffer");
    
    rfs = FileSystem.get(job);    
    this.mapId = mapId;
    
    currentThreadsNumber = 
      job.getInt("mapred.maxthreads.generate.mapoutput", -1);
    
    if (threadPool == null) {
      threadPool = new ThreadPoolExecutor(0, currentThreadsNumber,
                                          60L, TimeUnit.SECONDS,
                                          new SynchronousQueue<Runnable>());
    }
    
    closerThreadsNumber =
      job.getInt("mapred.maxthreads.partition.closer", -1);

    final int tmpKeyPrefixInts =
      job.getInt("mapr.map.keyprefix.ints", MIN_KEYPREFIX_INTS);

    if (tmpKeyPrefixInts > MAX_KEYPREFIX_INTS
     || tmpKeyPrefixInts < MIN_KEYPREFIX_INTS)
    {
     throw new IOException(tmpKeyPrefixInts + ": key prefix ints must be in "
       + "[" + MIN_KEYPREFIX_INTS + ", " + MAX_KEYPREFIX_INTS + "]");
    }

    keyPrefixLen = tmpKeyPrefixInts * INT_BYTES;
    prefixdata = new byte[keyPrefixLen];
    keyStart = KEYPREFIX + tmpKeyPrefixInts;
    valStart = keyStart + 1;
    acctSize = valStart + 1;
    recSize = (acctSize + 1) * INT_BYTES;
  }
  
  @SuppressWarnings("unchecked")
  public void init(MapOutputCollector.Context context
                  ) throws IOException, ClassNotFoundException {
    if (LOG.isDebugEnabled())
      LOG.debug("Initializing: " + this.getClass().getName());

    job = context.getJobConf();
    reporter = context.getReporter();
    mapTask = context.getMapTask();
    mapTask.setReportOutputSize(false);
    // MapR_TODO (SG)
    //mapOutputFile = mapTask.getMapOutputFile();
    mapOutputFile = ReflectionUtils.newInstance(MapRFsOutputFile.class, job);
    mapOutputFile.setConf(job);

    sortPhase = mapTask.getSortPhase();
    spilledRecordsCounter = reporter.getCounter(TaskCounter.SPILLED_RECORDS);
    partitions = job.getNumReduceTasks();
    
    // MapR
    initFS(getTaskID());

    //sanity checks
    final float spillper =
      job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
    final float recper = 
      job.getFloat("io.sort.record.percent", -1f);
    final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);
    indexCacheMemoryLimit = job.getInt(JobContext.INDEX_CACHE_MEMORY_LIMIT,
                                       INDEX_CACHE_MEMORY_LIMIT_DEFAULT);
    if (spillper > (float)1.0 || spillper <= (float)0.0) {
      throw new IOException("Invalid \"" + JobContext.MAP_SORT_SPILL_PERCENT +
          "\": " + spillper);
    }
    if (recper > (float)1.0 || recper < (float)0.01) {
      throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
    }
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
    }
    sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
          QuickSort.class, IndexedSorter.class), job);
    // buffers and accounting
    int maxMemUsage = sortmb << 20;
    //maxMemUsage -= maxMemUsage % METASIZE;
    //kvbuffer = new byte[maxMemUsage];
    // MapR_TODO (SG)
    int recordCapacity = (int)(maxMemUsage * recper);
    recordCapacity -= recordCapacity % recSize;
    kvbuffer = new byte[maxMemUsage - recordCapacity];
    bufvoid = kvbuffer.length;
    recordCapacity /= recSize;
    kvoffsetTotal = recordCapacity - (recordCapacity % partitions);
    oldKvoffsets = null;
    initKvOffsets();
    kvindices = new int[kvoffsetTotal * acctSize];

    kvmeta = ByteBuffer.wrap(kvbuffer)
       .order(ByteOrder.nativeOrder())
       .asIntBuffer();
    //setEquator(0);
    //bufstart = bufend = bufindex = equator;
    bufstart = bufend = bufindex = 0;
    kvstart = kvend = kvindex = 0;

    maxRec = kvmeta.capacity() / NMETA;
    softLimit = (int)(kvbuffer.length * spillper);
    bufferRemaining = softLimit;
    softRecordLimit = (int)(kvoffsetTotal * spillper);
    LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
    LOG.info("soft limit at " + softLimit);
    LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
    LOG.info("kvstart = " + kvstart + "; length = " + maxRec);

    // k/v serialization
    comparator = job.getOutputKeyComparator();
    keyClass = (Class<K>)job.getMapOutputKeyClass();
    valClass = (Class<V>)job.getMapOutputValueClass();
    serializationFactory = new SerializationFactory(job);
    keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(bb);
    valSerializer = serializationFactory.getSerializer(valClass);
    valSerializer.open(bb);

    // output counters
    mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter =
      reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter = reporter
        .getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);

    // compression
    if (job.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        job.getMapOutputCompressorClass(DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    } else {
      codec = null;
    }

    // combiner
    final Counters.Counter combineInputCounter =
      reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                           combineInputCounter,
                                           reporter, null);
    if (combinerRunner != null) {
      final Counters.Counter combineOutputCounter =
        reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
      combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, job);
    } else {
      combineCollector = null;
    }
    spillInProgress = false;
    minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
    spillThread.setDaemon(true);
    spillThread.setName("MapRSpillThread");
    spillLock.lock();
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("Starting spill thread");
      spillThread.start();
      while (!spillThreadRunning) {
        spillDone.await();
      }
    } catch (InterruptedException e) {
      throw new IOException("Spill thread failed to initialize", e);
    } finally {
      spillLock.unlock();
    }
    if (sortSpillException != null) {
      throw new IOException("Spill thread failed to initialize",
          sortSpillException);
    }
  }
  
  /**
   * Inner class wrapping valuebytes, used for appendRaw.
   */
  protected class InMemValBytes extends DataInputBuffer {
    private byte[] buffer;
    private int start;
    private int length;

    public void reset(byte[] buffer, int start, int length) {
      this.buffer = buffer;
      this.start = start;
      this.length = length;

      if (start + length > bufvoid) {
        this.buffer = new byte[this.length];
        final int taillen = bufvoid - start;
        System.arraycopy(buffer, start, this.buffer, 0, taillen);
        System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
        this.start = 0;
      }

      super.reset(this.buffer, this.start, this.length);
    }
  }


  class MapRResultIterator implements RawKeyValueIterator {
    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final InMemValBytes vbytes = new InMemValBytes();
    private KvOffset offsets;
    private int current = 0;
    private int end = 0;
        
    public MapRResultIterator(KvOffset ko) {
      this.offsets = ko;
      this.current = -1;
      this.end  = ko.size();
    }
    
    public boolean next() throws IOException {
      return ++current < end;
    }    
    
    public DataInputBuffer getKey() throws IOException {
      final int kvoff = offsets.get(current) * acctSize;
      keybuf.reset(kvbuffer, kvindices[kvoff + keyStart],
                   kvindices[kvoff + valStart] - kvindices[kvoff + keyStart]);
      return keybuf;
    }

    public DataInputBuffer getValue() throws IOException {
      getVBytesForOffset(offsets.get(current) * acctSize, vbytes);
      return vbytes;
    }
    

    public Progress getProgress() {
      return null;
    }
    
    public void close() { }
    
  } // class MapRResultIterator
  

  /* Class for variable int array */
  class KvOffset implements IndexedSortable {
    int currentCapacity = 0, initialCapacity = 0, maxCapacity = 0; 
    int[] offsets;
    int size;
  
    // pack both keys into a single array object
    // for better cache locality
    //
    byte[] prefixkey;
  
    public KvOffset(int iCap, int mCap) throws IllegalArgumentException {
      if (iCap < 0)
        throw new IllegalArgumentException("Illegal Capacity: "+ iCap);
      this.initialCapacity = iCap;
      this.maxCapacity = mCap > iCap? mCap: iCap;
      this.offsets = new int[this.initialCapacity];
      this.currentCapacity = this.initialCapacity;
      this.size = 0;
    }
  
    public void add(int val) throws IndexOutOfBoundsException {
      if (size >= currentCapacity) 
        expand();
      offsets[size++] = val;
    }
  
    public int get(int pos) throws IndexOutOfBoundsException {
      RangeCheck(pos);
      return offsets[pos];
    }
  
    public void reset() {
      size = 0;
    }
  
    public int size() { 
      return size; 
    }
  
    private void RangeCheck(int index) throws IndexOutOfBoundsException {
      if (index < 0 || index >= size)
        throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
    }
    
    private void expand() throws IndexOutOfBoundsException {
      // If over maxCapacity throw an error
      if (currentCapacity >= maxCapacity) {
        throw new IndexOutOfBoundsException(
            "KvOffset: Limit reached. Current Capacity: "+ currentCapacity);
      }
      // expand array by 1.5 times (taken from ArrayList)
      int newsize = (currentCapacity * 3)/2 + 1;
      
      if (newsize >= maxCapacity) {
        newsize = maxCapacity;
      }
      int[] newarray = new int[newsize];
      // copy array offsets to newarray from 0- size
      System.arraycopy(offsets, 0, newarray, 0, size);
      offsets = null; // Give it to gc 
      offsets = newarray;
      currentCapacity = newsize;
    }
  
    private int putIntPrefix(int keyInt, int offset) {
      final int what = kvindices[keyInt];
      prefixkey[offset++] = (byte) (what >>> 24);
      prefixkey[offset++] = (byte) (what >>> 16);
      prefixkey[offset++] = (byte) (what >>> 8);
      prefixkey[offset++] = (byte) (what);
      return offset;
    }
  
    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(int i, int j) {
      final int ii = offsets[i] * acctSize + KEYPREFIX;
      final int ij = offsets[j] * acctSize + KEYPREFIX;
  
      int p = 0;
      for (int keyInt = ii; keyInt < ii + keyPrefixLen / INT_BYTES; keyInt++) {
        p = putIntPrefix(keyInt, p);
      }
  
      for (int keyInt = ij; keyInt < ij + keyPrefixLen / INT_BYTES; keyInt++) {
        p = putIntPrefix(keyInt, p);
      }
  
      int resFromPartitionAndPrefix =
        WritableComparator.compareBytes(
            prefixkey, 0, keyPrefixLen,
            prefixkey, keyPrefixLen, keyPrefixLen);
  
      if (resFromPartitionAndPrefix != 0) {
        return resFromPartitionAndPrefix;
      }
  
      return comparator.compare(kvbuffer,
          kvindices[ii + keyStart],
          kvindices[ii + valStart] - kvindices[ii + keyStart],
          kvbuffer,
          kvindices[ij + keyStart],
          kvindices[ij + valStart] - kvindices[ij + keyStart]);
    }
  
    /**
     * Swap logical indices st i, j MOD offset capacity.
     * @see IndexedSortable#swap
     */
    public void swap(int i, int j) {
      int tmp = offsets[i];
      offsets[i] = offsets[j];
      offsets[j] = tmp;
    }
  } // class KvOffset
  
  
  private int getIntPrefix(int keyInt, int offset) {
    kvindices[keyInt] =
        ((prefixdata[offset++] & 0xFF) << 24)
      | ((prefixdata[offset++] & 0xFF) << 16)
      | ((prefixdata[offset++] & 0xFF) << 8)
      |  (prefixdata[offset++] & 0xFF);
    return offset;
  }
  
  private void saveKvOffsets(boolean flush) {
    if (flush) {
      // give old array to GC
      freeKvOffsets(oldKvoffsets);
      oldKvoffsets = null;
      oldKvoffsets = kvoffsets;
    } else {
      if (oldKvoffsets == null) {
        // change kvoffsets
        oldKvoffsets = kvoffsets;
        initKvOffsets();
      } else {
        // swap them
        KvOffset[] tmp = kvoffsets;
        kvoffsets = oldKvoffsets;
        oldKvoffsets = tmp;
      }
    }
  }

  private void freeKvOffsets(KvOffset[] ko) {
    if (ko != null) {
      for (int i = 0; i < partitions; i++) {
        ko[i]= null;
      }
      ko = null;
    }
  }

  @SuppressWarnings("unchecked")
  private void initKvOffsets() {
    kvoffsets = new MapRFsOutputBuffer.KvOffset[partitions];
    for (int i = 0; i < partitions; i++) {
      kvoffsets[i] = new KvOffset(kvoffsetTotal/(4 * partitions), kvoffsetTotal);
    }
  }
  
  private final class CloserThread extends Thread {
    private final FSDataOutputStream[] fsobjs = new FSDataOutputStream[ 100];
    private int h, t;

    // accessed after shutdown, hence no need for volatile according to JMM
    private Throwable closerException;

    private synchronized void add( FSDataOutputStream os) {
      while ( ( h + 1) % fsobjs.length == t ) {
        try {
          wait();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Spurious wakeup?", e);
          }
        }
      }

      fsobjs[ h] = os;
      if (++h >= fsobjs.length) h = 0;
      notify();
    }

    @Override
    public void run() {
      FSDataOutputStream os = null;
      try {
        while (true) {
          synchronized(this) {
            while (h == t) {
              try {
                this.wait();
              } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Spurious wakeup?", e);
                }
              }
            }
            os = fsobjs[ t];
            if (++t >= fsobjs.length) t = 0;
            this.notify();
          }
          if (os == null) return;
          try {
            os.close();
          } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
              LOG.error("Failed to close: " + os, e);
            }
            if (closerException == null) {
              closerException = e; // remember the original exception
            }
          }
        }
      } catch (Throwable fatal) {
        if (LOG.isErrorEnabled()) {
          LOG.error(this + "is exiting due to a fatal exception", fatal);
        }
        if (closerException == null) {
          closerException = fatal;
        }
      }
    }

    private void signalShutdown() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shutting down the closer thread:" + this);
      }
      add(null);
    }

    private void awaitTermination() throws IOException {
      while (isAlive()) {
        try {
          join();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Spurious wakeup?", e);
          }
        }
      }
      if (closerException != null) {
        throw new IOException(
          "Failure while closing map outputs.",
          closerException);
      }
    }
  } // class CloserThread

  private TaskAttemptID getTaskID() {
    return mapTask.getTaskID();
  }

  /**
   * Set the point from which meta and serialization data expand. The meta
   * indices are aligned with the buffer, so metadata never spans the ends of
   * the circular buffer.
   */
  private void setEquator(int pos) {
    equator = pos;
    // set index prior to first entry, aligned at meta boundary
    final int aligned = pos - (pos % METASIZE);
    kvindex =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
        "(" + (kvindex * 4) + ")");
  }

  /**
   * The spill is complete, so set the buffer and meta indices to be equal to
   * the new equator to free space for continuing collection. Note that when
   * kvindex == kvend == kvstart, the buffer is empty.
   */
  private void resetSpill() {
    final int e = equator;
    bufstart = bufend = e;
    final int aligned = e - (e % METASIZE);
    // set start/end to point to first meta record
    kvstart = kvend =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
      (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
  }

  /**
   * Compute the distance in bytes between two indices in the serialization
   * buffer.
   * @see #distanceTo(int,int,int)
   */
  final int distanceTo(final int i, final int j) {
    return distanceTo(i, j, kvbuffer.length);
  }

  /**
   * Compute the distance between two indices in the circular buffer given the
   * max distance.
   */
  int distanceTo(final int i, final int j, final int mod) {
    return i <= j
      ? j - i
      : mod - i + j;
  }

  /**
   * For the given meta position, return the offset into the int-sized
   * kvmeta buffer.
   */
  int offsetFor(int metapos) {
    return metapos * NMETA;
  }

  /**
   * Compare logical range, st i, j MOD offset capacity.
   * Compare by partition, then by key.
   * @see IndexedSortable#compare
   */
  public int compare(final int mi, final int mj) {
    final int kvi = offsetFor(mi % maxRec);
    final int kvj = offsetFor(mj % maxRec);
    final int kvip = kvmeta.get(kvi + PARTITION);
    final int kvjp = kvmeta.get(kvj + PARTITION);
    // sort by partition
    if (kvip != kvjp) {
      return kvip - kvjp;
    }
    // sort by key
    return comparator.compare(kvbuffer,
        kvmeta.get(kvi + KEYSTART),
        kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
        kvbuffer,
        kvmeta.get(kvj + KEYSTART),
        kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
  }

  final byte META_BUFFER_TMP[] = new byte[METASIZE];
  /**
   * Swap metadata for items i, j
   * @see IndexedSortable#swap
   */
  public void swap(final int mi, final int mj) {
    int iOff = (mi % maxRec) * METASIZE;
    int jOff = (mj % maxRec) * METASIZE;
    System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
    System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
    System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
  }

  /**
   * Inner class managing the spill of serialized records to disk.
   */
  protected class BlockingBuffer extends DataOutputStream {

    public BlockingBuffer() {
      super(new Buffer());
    }

    /**
     * Mark end of record. Note that this is required if the buffer is to
     * cut the spill in the proper place.
     */
    public int markRecord() {
      bufmark = bufindex;
      return bufindex;
    }

    /**
     * Set position from last mark to end of writable buffer, then rewrite
     * the data between last mark and kvindex.
     * This handles a special case where the key wraps around the buffer.
     * If the key is to be passed to a RawComparator, then it must be
     * contiguous in the buffer. This recopies the data in the buffer back
     * into itself, but starting at the beginning of the buffer. Note that
     * this method should <b>only</b> be called immediately after detecting
     * this condition. To call it at any other time is undefined and would
     * likely result in data loss or corruption.
     * @see #markRecord()
     */
    protected synchronized void shiftBufferedKey() throws IOException {
      // spillLock unnecessary; both kvend and kvindex are current
      int headbytelen = bufvoid - bufmark;
      bufvoid = bufmark;
      //final int kvbidx = 4 * kvindex;
      //final int kvbend = 4 * kvend;
      //final int avail =
      //  Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
      //if (bufindex + headbytelen < avail) {
      // MapR_TODO (SG)
      if (bufindex + headbytelen < bufstart) {
        System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
        System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
        bufindex += headbytelen;
        bufferRemaining -= kvbuffer.length - bufvoid;
      } else {
        byte[] keytmp = new byte[bufindex];
        System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
        bufindex = 0;
        out.write(kvbuffer, bufmark, headbytelen);
        out.write(keytmp);
      }
    }
  }

  public class Buffer extends OutputStream {
    private final byte[] scratch = new byte[1];

    @Override
    public synchronized void write(int v)
        throws IOException {
      scratch[0] = (byte)v;
      write(scratch, 0, 1);
    }

    /**
     * Attempt to write a sequence of bytes to the collection buffer.
     * This method will block if the spill thread is running and it
     * cannot write.
     * @throws MapBufferTooSmallException if record is too large to
     *    deserialize into the collection buffer.
     */
    @Override
    public synchronized void write(byte b[], int off, int len)
        throws IOException {
      boolean buffull = false;
      boolean wrap = false;
      spillLock.lock();
      try {
        do {
          if (sortSpillException != null) {
            throw (IOException)new IOException("Spill failed"
                ).initCause(sortSpillException);
          }

          // sufficient buffer space?
          if (bufstart <= bufend && bufend <= bufindex) {
            buffull = bufindex + len > bufvoid;
            wrap = (bufvoid - bufindex) + bufstart > len;
          } else {
            // bufindex <= bufstart <= bufend
            // bufend <= bufindex <= bufstart
            wrap = false;
            buffull = bufindex + len > bufstart;
          }

          if (kvstart == kvend) {
            // spill thread not running
            if (kvend != kvindex) {
              // we have records we can spill
              final boolean bufsoftlimit = (bufindex > bufend)
                ? bufindex - bufend > softLimit
                : bufend - bufindex < bufvoid - softLimit;
              if (bufsoftlimit || (buffull && !wrap)) {
                if (LOG.isInfoEnabled()) {
                  LOG.info("write: Spilling map output: buffer full = "
                         + bufsoftlimit);
                }
                startSpill();
              }
            } else if (buffull && !wrap) {
              // We have no buffered records, and this record is too large
              // to write into kvbuffer. We must spill it directly from
              // collect
              final int size = ((bufend <= bufindex)
                ? bufindex - bufend
                : (bufvoid - bufend) + bufindex) + len;
              bufstart = bufend = bufindex = bufmark = 0;
              kvstart = kvend = kvindex = 0;
              bufvoid = kvbuffer.length;
              throw new MapBufferTooSmallException(size + " bytes");
            }
          }

          if (buffull && !wrap) {
            try {
              while (kvstart != kvend) {
                reporter.progress();
                spillDone.await();
              }
            } catch (InterruptedException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting for spillDone interrupted.", e);
              }
              throw (IOException)new IOException(
                  "Buffer interrupted while waiting for the writer"
                  ).initCause(e);
            }
          }
        } while (buffull && !wrap);
      } finally {
        spillLock.unlock();
      }
      // here, we know that we have sufficient space to write
      if (buffull) {
        final int gaplen = bufvoid - bufindex;
        System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
        len -= gaplen;
        off += gaplen;
        bufindex = 0;
      }
      System.arraycopy(b, off, kvbuffer, bufindex, len);
      bufindex += len;
    }
  }

  public void close() { 
  }


  private final class MapRSpillThread extends Thread {
    private boolean shouldShutdown;

    @Override
    public void run() {
      spillLock.lock();
      spillThreadRunning = true;
      try {
        spilling:
        while (true) {
          spillDone.signal();
          while (kvstart == kvend) {
            try {
              spillReady.await();
            } catch (InterruptedException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Interrupted for shutdown? " + shouldShutdown, e);
              }
              if (shouldShutdown) {
                break spilling;
              }
            }
          }
          try {
            spillLock.unlock();
            sortAndSpill();
          } catch (Exception e) {
            sortSpillException = e;
          } catch (Throwable t) {
            sortSpillException = t;
            String logMsg = "Task " + getTaskID() + " failed : " 
                            + StringUtils.stringifyException(t);
            mapTask.reportFatalError(getTaskID(), t, logMsg);
          } finally {
            spillLock.lock();
            if (bufend < bufindex && bufindex < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
          }
        }
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }

    private void shutdown() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shutting down spill thread: " + this);
      }
      shouldShutdown = true;
      interrupt();
      while (isAlive()) {
        try {
          join();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted joining thread: " + this);
          }
        }
      }
    }
  }

  private void checkSpillException() throws IOException {
    final Throwable lspillException = sortSpillException;
    if (lspillException != null) {
      if (lspillException instanceof Error) {
        final String logMsg = "Task " + getTaskID() + " failed : " +
          StringUtils.stringifyException(lspillException);
        mapTask.reportFatalError(getTaskID(), lspillException, logMsg);
      }
      throw new IOException("Spill failed", lspillException);
    }
  }

  private synchronized void startSpill() {
    //assert !spillInProgress;
    kvend = kvindex;
    bufend = bufmark;
    //spillInProgress = true;
    saveKvOffsets(false);
    LOG.info("startSpill: Spilling map output");
    LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
             "; bufvoid = " + bufvoid);
    /*
    LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
             "); kvend = " + kvend + "(" + (kvend * 4) +
             "); length = " + (distanceTo(kvend, kvstart,
                    kvmeta.capacity()) + 1) + "/" + maxRec);
     */
    LOG.info("kvstart = " + kvstart + "; kvend = " + kvindex +
              "; length = " + kvoffsetTotal);
    spillReady.signal();
  }

  /**
   * Handles the degenerate case where serialization fails to fit in
   * the in-memory buffer, so we must spill the record from collect
   * directly to a spill file. Consider this "losing".
   */
  private void spillSingleRecord(final K key, final V value,
                                 int partition) throws IOException {
    long size = kvbuffer.length + partitions * MapTask.APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      // we don't run the combiner for a single record
      IndexRecord rec = new IndexRecord();
      for (int i = 0; i < partitions; ++i) {
        Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          // Create a new codec, don't care!
          writer = new Writer<K,V>(job, out, keyClass, valClass, codec,
                                          spilledRecordsCounter);

          if (i == partition) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            // Note that our map byte count will not be accurate with
            // compression
            mapOutputByteCounter.increment(out.getPos() - recordStart);
          }
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, i);

          writer = null;
        } catch (IOException e) {
          if (null != writer) writer.close();
          throw e;
        }
      }
      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }

  /**
   * Given an offset, populate vbytes with the associated set of
   * deserialized value bytes. Should only be called during a spill.
   */
  private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
    final int nextindex = (kvoff / acctSize ==
                          (kvend - 1 + kvoffsetTotal) % kvoffsetTotal)
      ? bufend
      : kvindices[(kvoff + acctSize + keyStart) % kvindices.length];
    int vallen = (nextindex >= kvindices[kvoff + valStart])
      ? nextindex - kvindices[kvoff + valStart]
      : (bufvoid - kvindices[kvoff + valStart]) + nextindex;
    vbytes.reset(kvbuffer, kvindices[kvoff + valStart], vallen);
  }

  
  protected boolean sortAndSpillWrapper() throws IOException, 
                                              ClassNotFoundException,
                                              InterruptedException {
	  if (numSpills == 0) {
	    if (LOG.isDebugEnabled()) {
		   LOG.debug("MapRFs: No spills");
	    }
	    // directly create output files
  		saveKvOffsets(true);
	  	sortAndWrite();
		  threadPool.shutdown();
		  return true;
	  } else {
		  if (LOG.isDebugEnabled()) {
		     LOG.debug("MapRFs: Number of spills: " + numSpills);
		  }
		  saveKvOffsets(true);
		  sortAndSpill();
	  }
	  
	  return false;
  }

  protected void sortAndSpill() throws IOException, 
                                       ClassNotFoundException,
                                       InterruptedException {
    //approximate the length of the output file to be the length of the
    //buffer + header lengths for the partitions
    long size = (bufend >= bufstart
        ? bufend - bufstart
        : (bufvoid - bufend) + bufstart) +
                partitions * MapTask.APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      out = rfs.createFid(mapOutputFile.getSpillFid(),
          mapOutputFile.getSpillFileForWriteFid(
                  org.apache.hadoop.mapred.TaskAttemptID.downgrade(mapId), numSpills, size));
      IndexRecord rec = new IndexRecord();
      InMemValBytes value = new InMemValBytes();

      final byte[] threadLocalPrefix = new byte[2 * keyPrefixLen];
      for (int i = 0; i < partitions; ++i) {
        final KvOffset kvo = oldKvoffsets[i];
        kvo.prefixkey = threadLocalPrefix;
        Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          writer = new Writer<K, V>(job, out, keyClass, valClass, codec,
                                                 spilledRecordsCounter);
          if (kvo.size() > 0) {
            // sort
            sorter.sort(kvo, 0, kvo.size(), reporter);
            if (combinerRunner == null) {
              // spill directly
              DataInputBuffer key = new DataInputBuffer();
              int j = 0;
              while (j < kvo.size()) {
                final int kvoff = kvo.get(j++) * acctSize;
                getVBytesForOffset(kvoff, value);
                key.reset(kvbuffer, kvindices[kvoff + keyStart],
                    (kvindices[kvoff + valStart] -
                     kvindices[kvoff + keyStart]));
                writer.append(key, value);
              }
            } else {
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition
              if (kvo.size() > 0) {
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter =
                  new MapRResultIterator(kvo);
                combinerRunner.combine(kvIter, combineCollector);
              }
            }
          }
          kvo.reset();
          // close the writer
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, i);
          writer = null;
        } finally {
          if (null != writer) writer.close();
        }
      }

      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename = mapOutputFile.getSpillIndexFileForWrite(
            numSpills, partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job, null, rfs);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("sortAndSpill: Finished spill " + numSpills + 
                 ", kvstart = " + kvstart + ", kvend = " + kvend);
      }
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }
  
  /** 
   * Sorts in memory buffer and writes output directly to local disk.
   * Used only for maprfs.
   */
  private void sortAndWrite() throws IOException, ClassNotFoundException,
	         InterruptedException {

    final Random random = new Random();

    final List<CloserThread> closerList = new ArrayList<CloserThread>(closerThreadsNumber);
    for ( int i = 0; i < closerThreadsNumber; i++) {
      final CloserThread closerThread = new CloserThread();
      closerThread.setDaemon(true);
      closerList.add(closerThread);
      closerThread.start();
    }

    List<Callable<Integer>> calls = new ArrayList<Callable<Integer>>();
    final int threadsNumber = currentThreadsNumber; 
    final int maxLoop = partitions / threadsNumber;
    for ( int k = 0; k < threadsNumber; k++ ) {
      final int someOff = k;

      final byte[] threadLocalPrefix = new byte[2 * keyPrefixLen];

      Callable<Integer> call = new Callable<Integer>(){
        
      @Override
      public Integer call() throws Exception {
        InMemValBytes value = new InMemValBytes();
        int endValue = maxLoop*(someOff+1);
        if ( someOff == (threadsNumber - 1)) {
          endValue = partitions;
        }
        for (int i = maxLoop*someOff ; i < endValue; ++i) {
          
          FSDataOutputStream finalOut = 
            rfs.createFid(mapOutputFile.getOutputFid(),
              mapOutputFile.getOutputFileForWriteFid(mapId, -1, i));
          Writer<K, V> writer = null;
          try {
            writer = new Writer<K, V>(job, finalOut, keyClass, valClass, codec,
                                          spilledRecordsCounter);
            final KvOffset kvo = oldKvoffsets[i];
            kvo.prefixkey = threadLocalPrefix;
            if (kvo.size() > 0) {
              // sort
              sorter.sort(kvo, 0, kvo.size(), reporter);
              if (combinerRunner == null) {
                // write directly
                DataInputBuffer key = new DataInputBuffer();
                int j = 0;
                while (j < kvo.size()) {
                  final int kvoff = kvo.get(j++) * acctSize;
                  getVBytesForOffset(kvoff, value);
                  key.reset(kvbuffer, kvindices[kvoff + keyStart],
                      (kvindices[kvoff + valStart] -
                       kvindices[kvoff + keyStart]));
                  writer.append(key, value);
                }
              } else {
                if (kvo.size() > 0) {
                  combineCollector.setWriter(writer);
                  RawKeyValueIterator kvIter =
                    new MapRResultIterator(kvo);
                  combinerRunner.combine(kvIter, combineCollector);
                }
              }
            }
            kvo.reset();
            // close the writer
            writer.close();
            outputSize += writer.getRawLength();
            writer = null;
        } finally {
          if (null != writer) writer.close();
        }
        int threadNumber = random.nextInt(closerThreadsNumber);
        closerList.get(threadNumber).add(finalOut);
      }
        return 0;
      }};
      calls.add(call);
    }
    try {
      if ( LOG.isDebugEnabled() ) {
        LOG.debug("Before threading");
      }
      List<Future<Integer>> futures = threadPool.invokeAll(calls);
      for ( Future<Integer> future : futures) {
        try {
          future.get();
        } catch (CancellationException ignore) {
        } catch (ExecutionException exp) {
          LOG.error("Exception while processing sortAndWrite", exp);
          throw new IOException(exp);
        }
      }
      if ( LOG.isDebugEnabled() ) {
        LOG.debug("After threading");
      }
    } catch (InterruptedException e) {
      LOG.error("Exception while trying to write map outputs", e);
      throw new IOException(e);
    } finally {
      for ( int i = 0; i < closerList.size(); i++) {
        closerList.get(i).signalShutdown();
      }
      for ( int i = 0; i < closerList.size(); i++) {
        closerList.get(i).awaitTermination();
      }
  
      if ( LOG.isDebugEnabled()) {
        LOG.debug("End of sortandWrite");
      }
    }
  }

  protected void mergeParts() throws IOException, InterruptedException, 
                                   ClassNotFoundException {
    
    LOG.info("mergeParts: Merging partitions");
    freeKvOffsets(kvoffsets);
    freeKvOffsets(oldKvoffsets);
    kvoffsets = oldKvoffsets = null;

    final Path[] filename = new Path[numSpills];
    CloserThread closerThread = new CloserThread();
    closerThread.setDaemon(true);
    closerThread.start();

    for (int i = 0; i < numSpills; i++) {
      filename[i] = mapOutputFile.getSpillFile(i);
    }
    
    // read in paged indices
    for (int i = indexCacheList.size(); i < numSpills; ++i) {
      Path indexFileName = mapOutputFile.getSpillIndexFile(i);
      indexCacheList.add(new SpillRecord(indexFileName, job, 
                  null, UserGroupInformation.getCurrentUser().getShortUserName(),
                  rfs));
    }

    sortPhase.addPhases(partitions); // Divide sort phase into sub-phases
    for (int parts = 0; parts < partitions; parts++) {
      // create o/p file
      FSDataOutputStream finalOut = rfs.createFid(
        mapOutputFile.getOutputFid(),
        mapOutputFile.getOutputFileForWriteFid(
                org.apache.hadoop.mapred.TaskAttemptID.downgrade(mapId), -1, parts));

      //create the segments to be merged
      List<Segment<K,V>> segmentList = new ArrayList<Segment<K, V>>
                                                           (numSpills);
      // for each spill 
      for (int i = 0; i < numSpills; i++) {
        IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

        // MapR
        Segment<K,V> s = new Segment<K,V>(job, rfs, filename[i], 
               indexRecord.startOffset, indexRecord.partLength, codec, true);
        segmentList.add(i, s);
        if (LOG.isDebugEnabled()) {
          LOG.debug("MapId=" + mapId + " Reducer=" + parts +
            "Spill =" + i + "(" + indexRecord.startOffset + "," +
            indexRecord.rawLength + ", " + indexRecord.partLength + ")");
        }
      }
      Path tmpDir = mapOutputFile.getLocalPathForWrite(mapId.toString(), -1);
      //merge
      @SuppressWarnings("unchecked")
      RawKeyValueIterator kvIter = Merger.merge(job, rfs,
        keyClass, valClass, codec,
        segmentList, Math.max(2, job.getInt("io.sort.factor", -1)),
        tmpDir,
        job.getOutputKeyComparator(), reporter, false,
        null, spilledRecordsCounter,
        sortPhase.phase(), TaskType.MAP);
      // write merged output to disk
      Writer<K, V> writer =
        new Writer<K, V>(job, finalOut, keyClass, valClass, codec, 
            spilledRecordsCounter);
      if (combinerRunner == null || numSpills < minSpillsForCombine) {
        Merger.writeFile(kvIter, writer, reporter, job);
      } else {
        combineCollector.setWriter(writer);
        combinerRunner.combine(kvIter, combineCollector);
      }
      //close
      writer.close();
      sortPhase.startNextPhase();
      outputSize += writer.getRawLength();
      writer = null;
      closerThread.add(finalOut); //finalOut.close();
      finalOut = null;
    }
    closerThread.signalShutdown();
    for (int i = 0; i < numSpills; i++) {
      rfs.delete(filename[i], true);
    }
    closerThread.awaitTermination();
  } 

  private void checkKeyValuePartition(K key, V value, int partition
                                     ) throws IOException {
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                             + keyClass.getName() + ", received "
                             + key.getClass().getName());
     }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
  }

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  public synchronized void collect(K key, V value, int partition
                                   ) throws IOException {
    reporter.progress();
    checkKeyValuePartition(key, value, partition);
    final int kvnext = (kvindex + 1) % kvoffsetTotal;
    spillLock.lock();
    try {
      boolean kvfull;
      do {
        if (sortSpillException != null) {
          throw (IOException)new IOException("Spill failed"
              ).initCause(sortSpillException);
        }
        // sufficient acct space
        kvfull = kvnext == kvstart;
        final boolean kvsoftlimit = ((kvnext > kvend)
            ? kvnext - kvend > softRecordLimit
            : kvend - kvnext <= kvoffsetTotal - softRecordLimit);
        if (kvstart == kvend && kvsoftlimit) {
          if (LOG.isInfoEnabled()) {
            LOG.info("collect: Spilling map output: record full = " + kvsoftlimit);
          }
          startSpill();
        }
        if (kvfull) {
          try {
            while (kvstart != kvend) {
              reporter.progress();
              spillDone.await();
            }
          } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Waiting for spillDone interrupted.", e);
            }
            throw (IOException)new IOException(
                "Collector interrupted while waiting for the writer"
                ).initCause(e);
          }
        }
      } while (kvfull);
    } finally {
      spillLock.unlock();
    }

    try {
      // serialize key bytes into buffer
      int keystart = bufindex;
      keySerializer.serialize(key);
      if (bufindex < keystart) {
        // wrapped the key; reset required
        bb.shiftBufferedKey();
        keystart = 0;
      }
      // serialize value bytes into buffer
      final int valstart = bufindex;
      valSerializer.serialize(value);
      int valend = bb.markRecord();

      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(valend >= keystart
          ? valend - keystart
          : (bufvoid - keystart) + valend);

      // update accounting info
      int ind = kvindex * acctSize;
      kvoffsets[partition].add(kvindex);
      final int first = ind + KEYPREFIX;
      final int last = first + keyPrefixLen / INT_BYTES;
      if (key instanceof HasRawComparablePrefix) {
        final HasRawComparablePrefix hrcpKey = (HasRawComparablePrefix)key;
        hrcpKey.getPrefix(prefixdata, 0, keyPrefixLen);
        for (int keyInt = first, p = 0; keyInt < last; keyInt++) {
          p = getIntPrefix(keyInt, p);
        }
      } else {
        for (int keyInt = first; keyInt < last; keyInt++) {
          kvindices[keyInt] = 0;
        }
      }
      kvindices[ind + keyStart] = keystart;
      kvindices[ind + valStart] = valstart;
      kvindex = kvnext;
    } catch (MapBufferTooSmallException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Record too large for in-memory buffer", e);
      }
      spillSingleRecord(key, value, partition);
      mapOutputRecordCounter.increment(1);
      return;
    }
  }
    
  public synchronized void flush() throws IOException, ClassNotFoundException,
         InterruptedException {

    if (!spillThreadRunning) {
      return;
    }

    LOG.info("flush: Starting flush of map output, kvstart = " + 
             kvstart + ", kvend = " + kvend);
    boolean done = false;
    spillLock.lock();
    try {
      //while (spillInProgress) {
      while (kvstart != kvend) {
        reporter.progress();
        spillDone.await();
      }
      checkSpillException();

      /*final int kvbend = 4 * kvend;
      if ((kvbend + METASIZE) % kvbuffer.length !=
          equator - (equator % METASIZE)) {
        // spill finished
        resetSpill();
      }*/
      if (kvindex != kvend) {
        // MapR_TODO (SG)
        //kvend = (kvindex + NMETA) % kvmeta.capacity();
        kvend = kvindex;
        bufend = bufmark;
        LOG.info("flush: Spilling map output");
        LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                 "; bufvoid = " + bufvoid);
        LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                 "); kvend = " + kvend + "(" + (kvend * 4) +
                 "); length = " + (distanceTo(kvend, kvstart,
                       kvmeta.capacity()) + 1) + "/" + maxRec);
        done = sortAndSpillWrapper();
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for the writer", e);
    } finally {
      spillLock.unlock();
      // If spillThread is not shutdown when exception is thrown
      // in close() it will try to do closeQuitely() in MapTask
      // which will cause task hanging if Exception is thrown
      // from this try block
      spillThread.shutdown();
    }
    assert !spillLock.isHeldByCurrentThread();
    // shut down spill thread and wait for it to exit. Since the preceding
    // ensures that it is finished with its work (and sortAndSpill did not
    // throw), we elect to use an interrupt instead of setting a flag.
    // Spilling simultaneously from this thread while the spill thread
    // finishes its work might be both a useful way to extend this and also
    // sufficient motivation for the latter approach.
    /*try {
      spillThread.interrupt();
      spillThread.join();
    } catch (InterruptedException e) {
      throw new IOException("Spill failed", e);
    }*/
    // MapR
    // Moving spill thread shutdown into "finally" block
    //spillThread.shutdown();
    // release sort buffer before the merge
    kvbuffer = null;
    freeKvOffsets(kvoffsets);
    freeKvOffsets(oldKvoffsets);
    kvoffsets = oldKvoffsets = null;
    if (!done)
      mergeParts();
    //MapR_TODO (SG)
    //Path outputPath = mapOutputFile.getOutputFile();
    //fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
  }

} // MapRFsOutputBuffer
