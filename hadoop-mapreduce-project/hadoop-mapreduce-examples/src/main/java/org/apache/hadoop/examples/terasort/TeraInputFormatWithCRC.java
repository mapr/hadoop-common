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

package org.apache.hadoop.examples.terasort;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.StringUtils;

/**
 * An input format that reads the first 10 characters of each line as the key
 * and the rest of the line as the value. Both key and value are represented
 * as Text.
 */
public class TeraInputFormatWithCRC extends FileInputFormat<Text,Text> {

  static final String PARTITION_FILENAME = "_partition.lst";
  static final String SAMPLE_SIZE = "terasort.partitions.sample";
  private static MRJobConfig lastContext = null;
  private static List<InputSplit> lastResult = null;


  static class TextSampler implements IndexedSortable {
    private ArrayList<Text> records = new ArrayList<Text>();

    public int compare(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      return left.compareTo(right);
    }

    public void swap(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      records.set(j, left);
      records.set(i, right);
    }

    public void addKey(Text key) {
      records.add(new Text(key));
    }

    /**
     * Find the split points for a given sample. The sample keys are sorted
     * and down sampled to find even split points for the partitions. The
     * returned keys should be the start of their respective partitions.
     * @param numPartitions the desired number of partitions
     * @return an array of size numPartitions - 1 that holds the split points
     */
    Text[] createPartitions(int numPartitions) {
      int numRecords = records.size();
      System.out.println("Making " + numPartitions + " from " + numRecords + 
                         " records");
      if (numPartitions > numRecords) {
        throw new IllegalArgumentException
          ("Requested more partitions than input keys (" + numPartitions +
           " > " + numRecords + ")");
      }
      new QuickSort().sort(this, 0, records.size());
      float stepSize = numRecords / (float) numPartitions;
      System.out.println("Step size is " + stepSize);
      Text[] result = new Text[numPartitions-1];
      for(int i=1; i < numPartitions; ++i) {
        result[i-1] = records.get(Math.round(stepSize * i));
      }
      return result;
    }
  }
  
  /**
   * Use the input splits to take samples of the input and generate sample
   * keys. By default reads 100,000 keys from 10 locations in the input, sorts
   * them and picks N-1 keys to generate N equally sized partitions.
   * @param conf the job to sample
   * @param partFile where to write the output file to
   * @throws IOException if something goes wrong
   */
  public static void writePartitionFile(final JobContext job, 
                                        Path partFile) throws IOException {
    TeraInputFormatWithCRC inFormat = new TeraInputFormatWithCRC();
    TextSampler sampler = new TextSampler();
    Configuration conf = job.getConfiguration();
    int partitions = job.getNumReduceTasks();
    long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
    List<InputSplit> splits = inFormat.getSplits(job);
    int samples = Math.min(10, splits.size());
    long recordsPerSample = sampleSize / samples;
    int sampleStep = splits.size() / samples;
    long records = 0;
    // take N samples from different parts of the input
    
    TaskAttemptContext context = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
    for(int i=0; i < samples; ++i) {
      try {
	      RecordReader<Text,Text> reader = 
	        inFormat.createRecordReader(splits.get(sampleStep * i), context);
	      reader.initialize(splits.get(sampleStep * i), context);
	      while (reader.nextKeyValue()) {
	      	if(reader.getCurrentKey().getLength() > 0){
	          sampler.addKey(new Text(reader.getCurrentKey()));
	          records += 1;
	          if ((i+1) * recordsPerSample <= records) {
	            break;
	          }
	      	}
	      }
      } catch (IOException ie){
          System.err.println("Got an exception while reading splits " +
              StringUtils.stringifyException(ie));
          throw new RuntimeException(ie);
      } catch(InterruptedException iex) {
    		
   	  }
    }
    FileSystem outFs = partFile.getFileSystem(conf);
    if (outFs.exists(partFile)) {
      outFs.delete(partFile, false);
    }
    DataOutputStream writer = outFs.create(partFile, true, 64*1024, (short) 10, 
        outFs.getDefaultBlockSize(partFile));
    for(Text split : sampler.createPartitions(partitions)) {
      split.write(writer);
    }
    writer.close();
  }

  static class TeraRecordReader extends RecordReader<Text,Text> {
    private static int KEY_LENGTH = 10;
    static final int VALUE_LENGTH = 90;
    private static int CRC_LENGTH = 20;
    private Text key;
    private Text value;
    private FSDataInputStream in;
    private long offset;
    private long length;
    private static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH + CRC_LENGTH;
    private byte[] buffer = new byte[RECORD_LENGTH];


    public TeraRecordReader() {
    	
    }
    
	@Override
	public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
        Path p = ((FileSplit)split).getPath();
        FileSystem fs = p.getFileSystem(context.getConfiguration());
        in = fs.open(p);
        long start = ((FileSplit)split).getStart();
        // find the offset to start at a record boundary
        offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
        in.seek(start + offset);
        length = ((FileSplit)split).getLength();
	}

    public void close() throws IOException {
      in.close();
    }

    public float getProgress() throws IOException {
      return (float) offset / length;
    }

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
      if (offset >= length) {
        return false;
      }
      int read = 0;
      while (read < RECORD_LENGTH) {
	      long newRead = in.read(buffer, read, RECORD_LENGTH - read);
	      if (newRead == -1) {
	        if (read == 0) {
	          return false;
	        } else {
	          throw new EOFException("read past eof");
	        }
	      }
	      read += newRead;
      }
      if ( buffer.length < KEY_LENGTH ) {
    	try {
	          throw new Exception("The length of the line is less than key length " + buffer.length);
          } catch (Exception e) {
	          // TODO Auto-generated catch block
	          e.printStackTrace();
          }
      } else {
	     if (key == null) {
	       key = new Text();
	     }
	     if (value == null) {
	       value = new Text();
	     }
         int actual_value_length = buffer.length - KEY_LENGTH - CRC_LENGTH;
         key.set(buffer, 0, KEY_LENGTH);
         value.set(buffer, KEY_LENGTH, actual_value_length);
         
         Long start_cksum = System.currentTimeMillis();
         CRC32 checksum = new CRC32();
         checksum.update(key.getBytes(), 0, key.getLength());
         checksum.update(value.getBytes(), 0, value.getLength());
        // checksum.update(bytes, KEY_LENGTH, actual_value_length);
         Long end_cksum = System.currentTimeMillis();
         
         String cksum_bytes = new String(buffer, KEY_LENGTH + actual_value_length, CRC_LENGTH).trim();
         long stored_cksum = Long.parseLong(cksum_bytes);
         
         long computed_cksum = checksum.getValue();
         String computed_cksum_str = Long.toString(computed_cksum);
         
         String rowid_str = new String(buffer, KEY_LENGTH+2, 32).trim();
         long rowid = Long.parseLong(rowid_str, 16);
         if(!computed_cksum_str.equals(cksum_bytes)) {
	     	try {
	            throw new Exception("CHECKSUM MISMATCH at row " + rowid + ": computed " 
	            		+ computed_cksum + " stored " + stored_cksum);
	          } catch (Exception e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	            System.exit(1);
	          }         	
	     }
      }
      offset += RECORD_LENGTH;
      return true;

	}
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    if (job == lastContext) {
      return lastResult;
    }
    lastContext = job;
    lastResult = super.getSplits(job);
    return lastResult;
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordReader<Text, Text> createRecordReader(
		org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
	return new TeraRecordReader();
  }
}
