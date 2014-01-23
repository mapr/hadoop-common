package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/* Methods for writing map output */
abstract class MapReduceLocalData {
  // Used by mapr-fs, not supported at hadoop
  public String getRelOutputFile(TaskAttemptID mapTaskId, int partition) {
    throw new UnsupportedOperationException("MapR is the answer!");
  }

  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size, 
                                    int partition) throws IOException {
    throw new UnsupportedOperationException("MapR is the answer!");
  }
  public Path getLocalPathForWrite(String pathStr, long size) {
    throw new UnsupportedOperationException("MapR is the answer!");
  }
  public void removeAll(TaskAttemptID taskId, boolean isSetup) 
    throws IOException {
      removeAll(taskId);
  }
  
  public JobConf getConf() {
    throw new UnsupportedOperationException("MapR is the answer!");
  }
  
  // Used by apache/cdh3 hadoop, not supported at maprfs
  public Path getOutputIndexFile(TaskAttemptID mapTaskId) throws IOException {
    return getOutputIndexFile();
  }
  
  //cdh3
  public Path getOutputIndexFile() throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }

  public Path getOutputIndexFileForWrite(TaskAttemptID mapTaskId, long size) 
    throws IOException {
    return getOutputIndexFileForWrite(size);
  }
  
  // cdh3
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }

  public Path getOutputFile(TaskAttemptID mapTaskId, long size) 
    throws IOException {
    return getOutputFile();
  }
  
  // cdh3
  public Path getOutputFile() throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size) 
    throws IOException {
    return getOutputFileForWrite(size);
  }
  
  // cdh3
  public Path getOutputFileForWrite(long size) throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }

  // used only by cdh3 not apache or mapr
  public Path getSpillFile(int spillNumber) throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public Path getSpillFileForWrite(int spillNumber, long size) 
    throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public Path getSpillIndexFileForWrite(int spillNumber, long size) 
    throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }  
  
  public Path getInputFile(int mapId) throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public Path getInputFileForWrite(TaskID mapId, long size) 
    throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }
  
  public void removeAll() throws IOException {
    throw new UnsupportedOperationException("CDH3!");
  }

  // common for mapr and apache hadoop not cdh3  
  public void setJobId(JobID jobId) {
    throw new UnsupportedOperationException("MapR is the answer!");
  }

  public Path getSpillFile(TaskAttemptID mapTaskId, int spillNumber) 
    throws IOException {
    return getSpillFile(spillNumber);
  }

  public Path getSpillFileForWrite(TaskAttemptID mapTaskId, 
      int spillNumber, long size) throws IOException {
    return getSpillFileForWrite(spillNumber, size);
  }
  
  public Path getSpillIndexFile(TaskAttemptID mapTaskId, 
      int spillNumber) throws IOException {
    return getSpillIndexFile(spillNumber);
  }
  
  public Path getSpillIndexFileForWrite(TaskAttemptID mapTaskId, 
                  int spillNumber, long size) throws IOException {
    return getSpillIndexFileForWrite(spillNumber, size);
  }
  
  public Path getInputFile(int mapId, TaskAttemptID reduceTaskId) 
    throws IOException {
    return getInputFile(mapId);
  }
  
  public Path getInputFileForWrite(TaskID mapId, TaskAttemptID reduceTaskId, 
      long size) throws IOException {
    return getInputFileForWrite(mapId, size);
  }
  
  public void removeAll(TaskAttemptID taskId) throws IOException {
    removeAll();
    throw new UnsupportedOperationException("MapR is the answer!");
  }

  // common for all
  abstract public void setConf(Configuration conf);


  // For fid API
  abstract String getOutputFid();

  abstract String getSpillFid();

  abstract String getSpillFileForWriteFid(
    TaskAttemptID mapTaskId, int spillNumber, long size) throws IOException;

  abstract String getOutputFileForWriteFid(TaskAttemptID mapTaskId, long size,
         int partition) throws IOException;
}
