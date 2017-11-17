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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestDirectShuffleMergeManager {

  @Test(timeout=10000)
  public void testMemoryMerge() throws Exception {
    final int TOTAL_MEM_BYTES = 10000;
    final int OUTPUT_SIZE = 7950;
    JobConf conf = new JobConf();
    conf.setFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 1.0f);
    conf.setLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES, TOTAL_MEM_BYTES);
    conf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.8f);
    conf.setFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 0.9f);
    conf.set("mapred.ifile.outputstream", "org.apache.hadoop.mapred.MapRIFileOutputStream");
    conf.set("mapred.ifile.inputstream", "org.apache.hadoop.mapred.MapRIFileInputStream");
    conf.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem");
    TestExceptionReporter reporter = new TestExceptionReporter();
    CyclicBarrier mergeStart = new CyclicBarrier(2);
    CyclicBarrier mergeComplete = new CyclicBarrier(2);
    final StubbedMergeManager mgr = new StubbedMergeManager(conf, reporter,
        mergeStart, mergeComplete);

    // reserve enough map output to cause a merge when it is committed
    MapOutput<Text, Text> out1 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                      (out1 instanceof DirectInMemoryOutput));
    DirectInMemoryOutput<Text, Text> mout1 = (DirectInMemoryOutput<Text, Text>)out1;
    fillOutput(mout1);
    MapOutput<Text, Text> out2 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                      (out2 instanceof DirectInMemoryOutput));
    DirectInMemoryOutput<Text, Text> mout2 = (DirectInMemoryOutput<Text, Text>)out2;
    fillOutput(mout2);

    // next reservation should be a WAIT
    Thread t = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          MapOutput<Text, Text> out3 = mgr.reserve(null, OUTPUT_SIZE, 0);
          // should wait here
          Assert.assertNotNull("Should be told to wait", out3);
          Assert.assertTrue("Should be a memory merge",
              (out3 instanceof DirectInMemoryOutput));
          DirectInMemoryOutput<Text, Text> mout3 = (DirectInMemoryOutput<Text, Text>)out3;
          fillOutput(mout3);
          mout3.commit();
        } catch (IOException e) {
          Assert.fail();
        }
        
      }});
    t.start();
    // trigger the first merge and wait for merge thread to start merging
    // and free enough output to reserve more
    mout1.commit();
    mout2.commit();
    t.join();
    mergeStart.await();

    Assert.assertEquals(1, mgr.getNumMerges());

    // reserve enough map output to cause another merge when committed
    out1 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                       (out1 instanceof DirectInMemoryOutput));
    mout1 = (DirectInMemoryOutput<Text, Text>)out1;
    fillOutput(mout1);
    mout1.commit();
    // allow the first merge to complete
    mergeComplete.await();

    out2 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                       (out2 instanceof DirectInMemoryOutput));
    mout2 = (DirectInMemoryOutput<Text, Text>)out2;
    fillOutput(mout2);

    // next reservation should be null
    Thread t1 = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          MapOutput<Text, Text> out3 = mgr.reserve(null, OUTPUT_SIZE, 0);
          // should wait here
          Assert.assertNotNull("Should be told to wait", out3);
          Assert.assertTrue("Should be a memory merge",
              (out3 instanceof DirectInMemoryOutput));
          out3 = (DirectInMemoryOutput<Text, Text>)out3;
          DirectInMemoryOutput<Text, Text> mout3 = (DirectInMemoryOutput<Text, Text>)out3;
          fillOutput(mout3);
          mout3.commit();
        } catch (IOException e) {
          Assert.fail();
        }
        
      }});
    t1.start();
    // commit output *before* merge thread completes
    mout2.commit();
    t1.join();

    // start the second merge and verify
    mergeStart.await();
    Assert.assertEquals(2, mgr.getNumMerges());

    // trigger the end of the second merge
    mergeComplete.await();

    Assert.assertEquals(3, mgr.getNumMerges());
    Assert.assertEquals("exception reporter invoked",
        0, reporter.getNumExceptions());
  }

  private void fillOutput(DirectInMemoryOutput<Text, Text> output) throws IOException {
    BoundedByteArrayOutputStream stream = output.getArrayStream();
    int count = stream.getLimit();
    for (int i=0; i < count; ++i) {
      stream.write(i);
    }
  }

  private static class StubbedMergeManager extends DirectShuffleMergeManagerImpl<Text, Text> {
    private TestMergeThread mergeThread;

    public StubbedMergeManager(JobConf conf, ExceptionReporter reporter,
        CyclicBarrier mergeStart, CyclicBarrier mergeComplete) throws IOException {
      super(null, conf, mock(LocalFileSystem.class), null, null, null,
          null, null, null, null, reporter, null, mock(MapOutputFile.class));
      mergeThread.setSyncBarriers(mergeStart, mergeComplete);
    }

    @Override
    protected MergeThread<DirectInMemoryOutput<Text, Text>, Text, Text> createInMemoryMerger() {
      mergeThread = new TestMergeThread(this, getExceptionReporter());
      return mergeThread;
    }

    public int getNumMerges() {
      return mergeThread.getNumMerges();
    }
  }

  private static class TestMergeThread
  extends MergeThread<DirectInMemoryOutput<Text,Text>, Text, Text> {
    private AtomicInteger numMerges;
    private CyclicBarrier mergeStart;
    private CyclicBarrier mergeComplete;

    public TestMergeThread(DirectShuffleMergeManagerImpl<Text, Text> mergeManager,
        ExceptionReporter reporter) {
      super(mergeManager, Integer.MAX_VALUE, reporter);
      numMerges = new AtomicInteger(0);
    }

    public synchronized void setSyncBarriers(
        CyclicBarrier mergeStart, CyclicBarrier mergeComplete) {
      this.mergeStart = mergeStart;
      this.mergeComplete = mergeComplete;
    }

    public int getNumMerges() {
      return numMerges.get();
    }

    @Override
    public void merge(List<DirectInMemoryOutput<Text, Text>> inputs)
        throws IOException {
      synchronized (this) {
        numMerges.incrementAndGet();
        for (DirectInMemoryOutput<Text, Text> input : inputs) {
          ((DirectShuffleMergeManagerImpl<Text, Text>) manager).unreserve(input.getSize());
        }
      }

      try {
        mergeStart.await();
        mergeComplete.await();
      } catch (InterruptedException e) {
      } catch (BrokenBarrierException e) {
      }
    }
  }

  private static class TestExceptionReporter implements ExceptionReporter {
    private List<Throwable> exceptions = new ArrayList<Throwable>();

    @Override
    public void reportException(Throwable t) {
      exceptions.add(t);
      t.printStackTrace();
    }

    public int getNumExceptions() {
      return exceptions.size();
    }
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test(timeout=10000)
  public void testOnDiskMerger() throws IOException, URISyntaxException,
    InterruptedException {
    JobConf jobConf = new JobConf();
    final int SORT_FACTOR = 5;
    jobConf.setInt(MRJobConfig.IO_SORT_FACTOR, SORT_FACTOR);
    jobConf.set("mapred.ifile.outputstream", "org.apache.hadoop.mapred.MapRIFileOutputStream");
    jobConf.set("mapred.ifile.inputstream", "org.apache.hadoop.mapred.MapRIFileInputStream");
    jobConf.set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem");

    MapOutputFile mapOutputFile = mock(MapOutputFile.class);
    doNothing().when(mapOutputFile).setConf(any(JobConf.class));

    FileSystem fs = FileSystem.getLocal(jobConf);
    DirectShuffleMergeManagerImpl<IntWritable, IntWritable> manager =
      new DirectShuffleMergeManagerImpl<IntWritable, IntWritable>(null, jobConf, fs, null
        , null, null, null, null, null, null, null, null, mapOutputFile);

    MergeThread<MapOutput<IntWritable, IntWritable>, IntWritable, IntWritable>
      onDiskMerger = (MergeThread<MapOutput<IntWritable, IntWritable>,
        IntWritable, IntWritable>) Whitebox.getInternalState(manager,
          "onDiskMerger");
    int mergeFactor = (Integer) Whitebox.getInternalState(onDiskMerger,
      "mergeFactor");

    // make sure the io.sort.factor is set properly
    assertEquals(mergeFactor, SORT_FACTOR);

    // Stop the onDiskMerger thread so that we can intercept the list of files
    // waiting to be merged.
    onDiskMerger.suspend();

    //Send the list of fake files waiting to be merged
    Random rand = new Random();
    for(int i = 0; i < 2*SORT_FACTOR; ++i) {
      Path path = new Path("somePath"); //+ rand.nextInt());
      FileStatus cap = new FileStatus(Math.abs(rand.nextInt()), false, 0,
          0, 0, path);
      manager.closeOnDiskFile(cap);
    }

    //Check that the files pending to be merged are in sorted order.
    LinkedList<List<FileStatus>> pendingToBeMerged =
      (LinkedList<List<FileStatus>>) Whitebox.getInternalState(
        onDiskMerger, "pendingToBeMerged");
    assertTrue("No inputs were added to list pending to merge",
      pendingToBeMerged.size() > 0);
    for(int i = 0; i < pendingToBeMerged.size(); ++i) {
      List<FileStatus> inputs = pendingToBeMerged.get(i);
      assertTrue("Not enough / too many inputs were going to be merged",
          inputs.size() > 0 && inputs.size() <= SORT_FACTOR);
      for(int j = 1; j < inputs.size(); ++j) {
        assertTrue("Inputs to be merged were not sorted according to size: ",
          inputs.get(j).getLen()
          >= inputs.get(j-1).getLen());
      }
    }

  }
}
