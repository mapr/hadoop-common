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

import java.io.FilterInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.TaskID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test that the Fetcher does what we expect it to.
 */
public class TestDirectShuffleFetcher {
  private static final Log LOG = LogFactory.getLog(TestDirectShuffleFetcher.class);
  JobConf job = null;
  TaskAttemptID id = null;
  DirectShuffleSchedulerImpl<Text, Text> ss = null;
  DirectShuffleMergeManagerImpl<Text, Text> mm = null;
  Reporter r = null;
  ShuffleClientMetrics metrics = null;
  ExceptionReporter except = null;
  Counters.Counter allErrs = null;
  PathId pathId;
  FileSystem rfs;

  final String encHash = "vFE234EIFCiBgYs2tCXY/SjT8Kg=";
  final MapHost host = new MapHost("localhost", "http://localhost:8080/");
  final TaskAttemptID map1ID = TaskAttemptID.forName("attempt_0_1_m_1_1");
  final TaskAttemptID map2ID = TaskAttemptID.forName("attempt_0_1_m_2_1");

  @Rule public TestName name = new TestName();

  @Before
  @SuppressWarnings("unchecked") // mocked generics
  public void setup() {
    LOG.info(">>>> " + name.getMethodName());
    job = new JobConf();
    id = TaskAttemptID.forName("attempt_0_1_r_1_1");
    ss = mock(DirectShuffleSchedulerImpl.class);
    mm = mock(DirectShuffleMergeManagerImpl.class);
    r = mock(Reporter.class);
    metrics = mock(ShuffleClientMetrics.class);
    except = mock(ExceptionReporter.class);
    rfs = mock(FileSystem.class);
    allErrs = mock(Counters.Counter.class);
    when(r.getCounter(anyString(), anyString())).thenReturn(allErrs);

    ArrayList<TaskAttemptID> maps = new ArrayList<TaskAttemptID>(1);
    maps.add(map1ID);
    maps.add(map2ID);
      pathId = mock(PathId.class);
      when(pathId.getFid()).thenReturn("anypath");
     


    MapOutputLocation loc = new MapOutputLocation(map1ID, "localhost", pathId);
    try {
      when(ss.getLocation()).thenReturn(loc);
    } catch (InterruptedException e) {
      fail();
    }
  }

  @After
  public void teardown() {
    LOG.info("<<<< " + name.getMethodName());
  }
  
  @Test
  public void testReduceOutOfDiskSpace() throws Throwable {
    LOG.info("testReduceOutOfDiskSpace");
    MapOutputFile mof = mock(MapRFsOutputFile.class);
    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(1, job, id, ss, mm,
        r, metrics, except, mof, rfs);

    final String content = "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n";
    long mOutL = content.getBytes().length;
    long dL = content.getBytes().length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    bos.write(content.getBytes());

    byte [] writeBuffer = new byte[8];
    writeBuffer[0] = (byte)(mOutL >>> 56);
    writeBuffer[1] = (byte)(mOutL >>> 48);
    writeBuffer[2] = (byte)(mOutL >>> 40);
    writeBuffer[3] = (byte)(mOutL >>> 32);
    writeBuffer[4] = (byte)(mOutL >>> 24);
    writeBuffer[5] = (byte)(mOutL >>> 16);
    writeBuffer[6] = (byte)(mOutL >>>  8);
    writeBuffer[7] = (byte)(mOutL >>>  0);
    
    bos.write(writeBuffer);

    writeBuffer[0] = (byte)(dL >>> 56);
    writeBuffer[1] = (byte)(dL >>> 48);
    writeBuffer[2] = (byte)(dL >>> 40);
    writeBuffer[3] = (byte)(dL >>> 32);
    writeBuffer[4] = (byte)(dL >>> 24);
    writeBuffer[5] = (byte)(dL >>> 16);
    writeBuffer[6] = (byte)(dL >>>  8);
    writeBuffer[7] = (byte)(dL >>>  0);

    bos.write(writeBuffer);

    final byte[] myB = bos.toByteArray();
    final FakeFSDataInputStream in = new FakeFSDataInputStream(
        bos.toByteArray());
    FSDataInputStream fsin = new FSDataInputStream(in){
      @Override
      public long getFileLength() {
        return myB.length;
      }
    };
    try {
        when(rfs.openFid2(any(PathId.class), anyString(), anyInt())).thenReturn(fsin);
      } catch (IOException e1) {
        fail();
      }
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
    .thenThrow(new IOException("No disk space available"));
  
    MapOutputLocation loc = ss.getLocation();
    underTest.copyOutput(loc);
    verify(ss).reportLocalError(any(IOException.class));
    verify(ss).addKnownMapOutput("localhost", map1ID, pathId);
  }
  /*
  @Test(timeout=30000)
  public void testCopyFromHostConnectionTimeout() throws Exception {
    when(connection.getInputStream()).thenThrow(
        new SocketTimeoutException("This is a fake timeout :)"));
    
    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    underTest.copyFromHost(host);
    
    verify(connection).addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    
    verify(allErrs).increment(1);
    verify(ss).copyFailed(map1ID, host, false, false);
    verify(ss).copyFailed(map2ID, host, false, false);
    
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map1ID));
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map2ID));
  }
  */
  
  @Test
  public void testCopyFailed() throws Exception {
    LOG.info("testCopyFailed");
    MapOutputFile mof = mock(MapRFsOutputFile.class);
    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(1, job, id, ss, mm,
        r, metrics, except, mof, rfs);
    
        when(rfs.openFid2(any(PathId.class), any(String.class), anyInt())).thenThrow(new FileNotFoundException());

        MapOutputLocation loc = ss.getLocation();
        underTest.copyOutput(loc);
        verify(ss).copyFailed(map1ID, loc);
        verify(ss).addKnownMapOutput("localhost", map1ID, pathId);
        verify(except, never()).reportException(any(IOException.class));

  }
  @Test(timeout=10000)
  public void testCopyFromHostWait() throws Exception {

    final String content = "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n";
    long mOutL = content.getBytes().length;
    long dL = content.getBytes().length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    bos.write(content.getBytes());

    byte [] writeBuffer = new byte[8];
    writeBuffer[0] = (byte)(mOutL >>> 56);
    writeBuffer[1] = (byte)(mOutL >>> 48);
    writeBuffer[2] = (byte)(mOutL >>> 40);
    writeBuffer[3] = (byte)(mOutL >>> 32);
    writeBuffer[4] = (byte)(mOutL >>> 24);
    writeBuffer[5] = (byte)(mOutL >>> 16);
    writeBuffer[6] = (byte)(mOutL >>>  8);
    writeBuffer[7] = (byte)(mOutL >>>  0);
    
    bos.write(writeBuffer);

    writeBuffer[0] = (byte)(dL >>> 56);
    writeBuffer[1] = (byte)(dL >>> 48);
    writeBuffer[2] = (byte)(dL >>> 40);
    writeBuffer[3] = (byte)(dL >>> 32);
    writeBuffer[4] = (byte)(dL >>> 24);
    writeBuffer[5] = (byte)(dL >>> 16);
    writeBuffer[6] = (byte)(dL >>>  8);
    writeBuffer[7] = (byte)(dL >>>  0);

    bos.write(writeBuffer);

    final byte[] myB = bos.toByteArray();
    final FakeFSDataInputStream in = new FakeFSDataInputStream(
        bos.toByteArray());
    FSDataInputStream fsin = new FSDataInputStream(in){
      @Override
      public long getFileLength() {
        return myB.length;
      }
    };
    try {
        when(rfs.openFid2(any(PathId.class), anyString(), anyInt())).thenReturn(fsin);
      } catch (IOException e1) {
        fail();
      }

    final DirectInMemoryOutput<Text,Text> immo = spy(new DirectInMemoryOutput<Text,Text>(
        job, id, mm, 100, null, true));
    when(mm.unconditionalReserve(map1ID, 2000000, true)).thenCallRealMethod();
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt())).thenCallRealMethod();
    when(mm.canShuffleToMemory(anyLong())).thenReturn(true);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock ignore) throws Throwable {
        ignore.callRealMethod();
        return null;
      }
    }).when(mm).unreserve(2000000);

    MapOutputFile mof = mock(MapRFsOutputFile.class);
    final DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(0, job, id, ss, mm,
        r, metrics, except, mof, rfs);
    mm.unconditionalReserve(map1ID, 2000000, true);
    final MapOutputLocation loc = ss.getLocation();
    Thread t = new Thread(new Runnable(){

      @Override
      public void run() {
        try {
          when(mm.unconditionalReserve(map1ID, 36, true)).thenReturn(immo);
          underTest.copyOutput(loc);
        } catch (IOException e) {
          fail();
        } catch (InterruptedException e) {
          fail();
        }        
      }});
    t.start();
    Thread.sleep(1000);
    mm.unreserve(2000000);
    t.join();
    
    verify(allErrs, times(1)).increment(1);
    verify(ss, times(1)).copyFailed(map1ID, loc);
    
    verify(ss).addKnownMapOutput("localhost", map1ID, pathId);
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=10000) 
  public void testCopyFromHostCompressFailure() throws Exception {
    final String content = "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n";
    long mOutL = content.getBytes().length;
    long dL = content.getBytes().length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    bos.write(content.getBytes());

    byte [] writeBuffer = new byte[8];
    writeBuffer[0] = (byte)(mOutL >>> 56);
    writeBuffer[1] = (byte)(mOutL >>> 48);
    writeBuffer[2] = (byte)(mOutL >>> 40);
    writeBuffer[3] = (byte)(mOutL >>> 32);
    writeBuffer[4] = (byte)(mOutL >>> 24);
    writeBuffer[5] = (byte)(mOutL >>> 16);
    writeBuffer[6] = (byte)(mOutL >>>  8);
    writeBuffer[7] = (byte)(mOutL >>>  0);
    
    bos.write(writeBuffer);

    writeBuffer[0] = (byte)(dL >>> 56);
    writeBuffer[1] = (byte)(dL >>> 48);
    writeBuffer[2] = (byte)(dL >>> 40);
    writeBuffer[3] = (byte)(dL >>> 32);
    writeBuffer[4] = (byte)(dL >>> 24);
    writeBuffer[5] = (byte)(dL >>> 16);
    writeBuffer[6] = (byte)(dL >>>  8);
    writeBuffer[7] = (byte)(dL >>>  0);

    bos.write(writeBuffer);

    final byte[] myB = bos.toByteArray();
    final FakeFSDataInputStream in = new FakeFSDataInputStream(
        bos.toByteArray());
    FSDataInputStream fsin = new FSDataInputStream(in){
      @Override
      public long getFileLength() {
        return myB.length;
      }
    };
    try {
        when(rfs.openFid2(any(PathId.class), anyString(), anyInt())).thenReturn(fsin);
      } catch (IOException e1) {
        fail();
      }

    
    DirectInMemoryOutput<Text, Text> immo = mock(DirectInMemoryOutput.class);
    MapOutputFile mof = mock(MapRFsOutputFile.class);
    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(0, job, id, ss, mm,
        r, metrics, except, mof, rfs);
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(immo);
    
    doThrow(new java.lang.InternalError()).when(immo)
        .shuffle(any(MapHost.class), any(InputStream.class), anyLong(), 
            anyLong(), any(ShuffleClientMetrics.class), any(Reporter.class));

    MapOutputLocation loc = ss.getLocation();
    underTest.copyOutput(loc);
       
    verify(ss, times(1)).copyFailed(map1ID, loc);
  }

  @Test(timeout=10000)
  public void testInterruptInMemory() throws Exception {
    final int FETCHER = 2;
    DirectInMemoryOutput<Text,Text> immo = spy(new DirectInMemoryOutput<Text,Text>(
          job, id, mm, 100, null, true));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(immo);
    doNothing().when(mm).waitForResource();

    final String content = "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n";
    long mOutL = content.getBytes().length;
    long dL = content.getBytes().length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    bos.write(content.getBytes());

    byte [] writeBuffer = new byte[8];
    writeBuffer[0] = (byte)(mOutL >>> 56);
    writeBuffer[1] = (byte)(mOutL >>> 48);
    writeBuffer[2] = (byte)(mOutL >>> 40);
    writeBuffer[3] = (byte)(mOutL >>> 32);
    writeBuffer[4] = (byte)(mOutL >>> 24);
    writeBuffer[5] = (byte)(mOutL >>> 16);
    writeBuffer[6] = (byte)(mOutL >>>  8);
    writeBuffer[7] = (byte)(mOutL >>>  0);
    
    bos.write(writeBuffer);

    writeBuffer[0] = (byte)(dL >>> 56);
    writeBuffer[1] = (byte)(dL >>> 48);
    writeBuffer[2] = (byte)(dL >>> 40);
    writeBuffer[3] = (byte)(dL >>> 32);
    writeBuffer[4] = (byte)(dL >>> 24);
    writeBuffer[5] = (byte)(dL >>> 16);
    writeBuffer[6] = (byte)(dL >>>  8);
    writeBuffer[7] = (byte)(dL >>>  0);

    bos.write(writeBuffer);

    final byte[] myB = bos.toByteArray();
    final StuckInputStream in = new StuckInputStream(
        bos.toByteArray());
    FSDataInputStream fsin = new FSDataInputStream(in){
      @Override
      public long getFileLength() {
        return myB.length;
      }
    };
    try {
        when(rfs.openFid2(any(PathId.class), anyString(), anyInt())).thenReturn(fsin);
      } catch (IOException e1) {
        fail();
      }

    MapOutputFile mof = mock(MapRFsOutputFile.class);
    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(FETCHER, job, id, ss, mm,
        r, metrics, except, mof, rfs);
    underTest.start();
    // wait for read in inputstream
    in.waitForFetcher();
    underTest.shutDown();
    underTest.join(); // rely on test timeout to kill if stuck

    assertTrue(in.wasClosedProperly());
    verify(immo).abort();
    verify(ss).copyFailed(any(TaskAttemptID.class), any(MapOutputLocation.class));
  }

  @Test(timeout=10000)
  public void testInterruptOnDisk() throws Exception {
    final int FETCHER = 7;
    Path p = new Path("file:///tmp/foo");
    Path pTmp = DirectOnDiskMapOutput.getTempPath(p, FETCHER);
    FileSystem mFs = mock(FileSystem.class, RETURNS_DEEP_STUBS);
    MapOutputFile mof = mock(MapRFsOutputFile.class);
    when(mof.getInputFileForWrite(any(TaskID.class), anyLong())).thenReturn(p);
    DirectOnDiskMapOutput<Text,Text> odmo = spy(new DirectOnDiskMapOutput<Text,Text>(map1ID,
        id, mm, 100L, job, mof, FETCHER, true, mFs, p));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(odmo);
    doNothing().when(mm).waitForResource();

    final String content = "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n";
    long mOutL = content.getBytes().length;
    long dL = content.getBytes().length;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
    bos.write(content.getBytes());

    byte [] writeBuffer = new byte[8];
    writeBuffer[0] = (byte)(mOutL >>> 56);
    writeBuffer[1] = (byte)(mOutL >>> 48);
    writeBuffer[2] = (byte)(mOutL >>> 40);
    writeBuffer[3] = (byte)(mOutL >>> 32);
    writeBuffer[4] = (byte)(mOutL >>> 24);
    writeBuffer[5] = (byte)(mOutL >>> 16);
    writeBuffer[6] = (byte)(mOutL >>>  8);
    writeBuffer[7] = (byte)(mOutL >>>  0);
    
    bos.write(writeBuffer);

    writeBuffer[0] = (byte)(dL >>> 56);
    writeBuffer[1] = (byte)(dL >>> 48);
    writeBuffer[2] = (byte)(dL >>> 40);
    writeBuffer[3] = (byte)(dL >>> 32);
    writeBuffer[4] = (byte)(dL >>> 24);
    writeBuffer[5] = (byte)(dL >>> 16);
    writeBuffer[6] = (byte)(dL >>>  8);
    writeBuffer[7] = (byte)(dL >>>  0);

    bos.write(writeBuffer);

    final byte[] myB = bos.toByteArray();
    final StuckInputStream in = new StuckInputStream(
        bos.toByteArray());
    FSDataInputStream fsin = new FSDataInputStream(in){
      @Override
      public long getFileLength() {
        return myB.length;
      }
    };
    try {
        when(rfs.openFid2(any(PathId.class), anyString(), anyInt())).thenReturn(fsin);
      } catch (IOException e1) {
        fail();
      }

    DirectShuffleFetcher<Text,Text> underTest = new FakeDirectShuffleFetcher<Text,Text>(FETCHER, job, id, ss, mm,
        r, metrics, except, mof, rfs);
    underTest.start();
    // wait for read in inputstream
    in.waitForFetcher();
    underTest.shutDown();
    underTest.join(); // rely on test timeout to kill if stuck

    assertTrue(in.wasClosedProperly());
    verify(mFs).create(eq(pTmp));
    verify(mFs).delete(eq(pTmp), eq(false));
    verify(odmo).abort();
    verify(ss).copyFailed(any(TaskAttemptID.class), any(MapOutputLocation.class));
  }

  public static class FakeDirectShuffleFetcher<K,V> extends DirectShuffleFetcher<K,V> {

    public FakeDirectShuffleFetcher(int id, JobConf job, TaskAttemptID reduceId,
        DirectShuffleSchedulerImpl<K,V> scheduler, DirectShuffleMergeManagerImpl<K,V> merger,
        Reporter reporter, ShuffleClientMetrics metrics,
        ExceptionReporter exceptionReporter, MapOutputFile mapOutputFile, FileSystem rfs) {
      super(id, job, reduceId, scheduler, merger, reporter, metrics,
          exceptionReporter, mapOutputFile);
      this.rfs = rfs;
    }

    
  }

  static class StuckInputStream extends FilterInputStream 
  implements Seekable, PositionedReadable {

    boolean stuck = false;
    volatile boolean closed = false;
    int length;

    StuckInputStream(byte[] barray) {
      super(new ByteArrayInputStream(barray));
      this.length = barray.length;
     }

    int freeze() throws IOException {
      synchronized (this) {
        stuck = true;
        notify();
      }
      // connection doesn't throw InterruptedException, but may return some
      // bytes geq 0 or throw an exception
      while (!Thread.currentThread().isInterrupted() || closed) {
        // spin
        if (closed) {
          throw new IOException("underlying stream closed, triggered an error");
        }
      }
      return 0;
    }

 /*   @Override
    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }
*/
    @Override
    public int read(byte[] b) throws IOException {
      int ret = super.read(b);
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }

    @Override
    public int read(byte[] b, int off, int len)  throws IOException {
      int ret = super.read(b, off, len);
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    public synchronized void waitForFetcher() throws InterruptedException {
      while (!stuck) {
        wait();
      }
    }

    public boolean wasClosedProperly() {
      return closed;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int ret = super.read(buffer, offset, length);
      if (ret != -1) {
        return ret;
      }
      return freeze();

    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int ret = super.read(buffer, offset, length);
      if (ret != -1) {
        return;
      }
      freeze();

    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      int ret = super.read(buffer);
      if (ret != -1) {
        return;
      }
      freeze();
    }

    @Override
    public void seek(long pos) throws IOException {
      // not really good one
      this.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
       return Math.abs(this.available() - length);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      // TODO Auto-generated method stub
      return false;
    }

  }

  public static class FakeFSDataInputStream extends ByteArrayInputStream 
  implements Seekable, PositionedReadable {

    public FakeFSDataInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      this.pos = (int) pos;
    }

    @Override
    public long getPos() throws IOException {
      return this.pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      // TODO Auto-generated method stub
      
    }

  }
}
