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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.terasort.TeraGen.Counters;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generate the official terasort input data set.
 * The user specifies the number of rows and the output directory and this
 * class runs a map/reduce program to generate the data.
 * The format of the data is:
 * <ul>
 * <li>(10 bytes key) (10 bytes rowid) (78 bytes filler) \r \n
 * <li>The keys are random characters from the set ' ' .. '~'.
 * <li>The rowid is the right justified row id as a int.
 * <li>The filler consists of 7 runs of 10 characters from 'A' to 'Z'.
 * </ul>
 *
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar teragen 10000000000 in-dir</b>
 */
public class TeraGenWithCRC extends Configured implements Tool {
	
	static Logger LOG = LoggerFactory.getLogger(TeraGenWithCRC.class);

  /**
   * An input format that assigns ranges of longs to each mapper.
   */
  static class RangeInputFormat 
       extends InputFormat<LongWritable, NullWritable> {
    
    /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit extends InputSplit implements Writable {
      long firstRow;
      long rowCount;

      public RangeInputSplit() { }

      public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
      }

      public long getLength() throws IOException {
        return 0;
      }

      public String[] getLocations() throws IOException {
        return new String[]{};
      }

      public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
      }
    }
    
    /**
     * A record reader that will generate a range of numbers.
     */
    static class RangeRecordReader 
        extends RecordReader<LongWritable, NullWritable> {
      long startRow;
      long finishedRows;
      long totalRows;
      LongWritable key = null;

      public RangeRecordReader() {
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) 
          throws IOException, InterruptedException {
        startRow = ((RangeInputSplit)split).firstRow;
        finishedRows = 0;
        totalRows = ((RangeInputSplit)split).rowCount;
      }

      public void close() throws IOException {
        // NOTHING
      }

      public LongWritable getCurrentKey() {
        return key;
      }

      public NullWritable getCurrentValue() {
        return NullWritable.get();
      }

      public float getProgress() throws IOException {
        return finishedRows / (float) totalRows;
      }

      public boolean nextKeyValue() {
        if (key == null) {
          key = new LongWritable();
        }
        if (finishedRows < totalRows) {
          key.set(startRow + finishedRows);
          finishedRows += 1;
          return true;
        } else {
          return false;
        }
      }
      
    }

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<LongWritable, NullWritable> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RangeRecordReader();
	}

	@Override
	public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(
			JobContext context) throws IOException, InterruptedException {
	      long totalRows = getNumberOfRows(context);
	      int numSplits = context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
	      long rowsPerSplit = totalRows / numSplits;
	      System.out.println("Generating " + totalRows + " using " + numSplits + 
	                         " maps with step of " + rowsPerSplit);
	      List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
	      long currentRow = 0;
	      for(int split=0; split < numSplits-1; ++split) {
	        splits.add(new RangeInputSplit(currentRow, rowsPerSplit));
	        currentRow += rowsPerSplit;
	      }
	      splits.add(new RangeInputSplit(currentRow, totalRows - currentRow));
	      return splits;
	}

  }
  
  static long getNumberOfRows(JobContext job) {
    return job.getConfiguration().getLong("terasort.num-rows", 0);
  }
  
  static void setNumberOfRows(Job job, long numRows) {
    job.getConfiguration().setLong("terasort.num-rows", numRows);
  }

  static class RandomGenerator {
    private long seed = 0;
    private static final long mask32 = (1l<<32) - 1;
    /**
     * The number of iterations separating the precomputed seeds.
     */
    private static final int seedSkip = 128 * 1024 * 1024;
    /**
     * The precomputed seed values after every seedSkip iterations.
     * There should be enough values so that a 2**32 iterations are 
     * covered.
     */
    private static final long[] seeds = new long[]{0L,
                                                   4160749568L,
                                                   4026531840L,
                                                   3892314112L,
                                                   3758096384L,
                                                   3623878656L,
                                                   3489660928L,
                                                   3355443200L,
                                                   3221225472L,
                                                   3087007744L,
                                                   2952790016L,
                                                   2818572288L,
                                                   2684354560L,
                                                   2550136832L,
                                                   2415919104L,
                                                   2281701376L,
                                                   2147483648L,
                                                   2013265920L,
                                                   1879048192L,
                                                   1744830464L,
                                                   1610612736L,
                                                   1476395008L,
                                                   1342177280L,
                                                   1207959552L,
                                                   1073741824L,
                                                   939524096L,
                                                   805306368L,
                                                   671088640L,
                                                   536870912L,
                                                   402653184L,
                                                   268435456L,
                                                   134217728L,
                                                  };

    /**
     * Start the random number generator on the given iteration.
     * @param initalIteration the iteration number to start on
     */
    RandomGenerator(long initalIteration) {
      int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
      seed = seeds[baseIndex];
      for(int i=0; i < initalIteration % seedSkip; ++i) {
        next();
      }
    }

    RandomGenerator() {
      this(0);
    }

    long next() {
      seed = (seed * 3141592621l + 663896637) & mask32;
      return seed;
    }
  }

  /**
   * The Mapper class that given a row number, will generate the appropriate 
   * output line.
   */
  public static class SortGenMapper 
      extends Mapper<LongWritable, NullWritable, Text, Text> {

    private Text key = new Text();
    private Text value = new Text();
    private Unsigned16 rand = null;
    private Unsigned16 rowId = null;
    private Unsigned16 checksum = new Unsigned16();
    private Checksum crc32 = new PureJavaCrc32();
    private Unsigned16 total = new Unsigned16();
    private static final Unsigned16 ONE = new Unsigned16(1);
    private byte[] buffer = new byte[TeraInputFormat.KEY_LENGTH +
                                     TeraInputFormat.VALUE_LENGTH];
    private Counter checksumCounter;

    public void map(LongWritable row, NullWritable ignored,
        Context context) throws IOException, InterruptedException {
      if (rand == null) {
        rowId = new Unsigned16(row.get());
        rand = Random16.skipAhead(rowId);
        checksumCounter = context.getCounter(Counters.CHECKSUM);
      }
      Random16.nextRand(rand);
      GenSort.generateRecord(buffer, rand, rowId);
      key.set(buffer, 0, TeraInputFormat.KEY_LENGTH);
      value.set(buffer, TeraInputFormat.KEY_LENGTH, 
                TeraInputFormat.VALUE_LENGTH);
      context.write(key, value);
      crc32.reset();
      crc32.update(buffer, 0, 
                   TeraInputFormat.KEY_LENGTH + TeraInputFormat.VALUE_LENGTH);
      checksum.set(crc32.getValue());
      total.add(checksum);
      rowId.add(ONE);
    }

    @Override
    public void cleanup(Context context) {
      if (checksumCounter != null) {
        checksumCounter.increment(total.getLow8());
      }
    }
  }


  /**
   * @param args the cli arguments
 * @throws InterruptedException 
 * @throws ClassNotFoundException 
   */
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //JobConf job = (JobConf) getConf();
	  Job job = Job.getInstance(getConf());
    setNumberOfRows(job, Long.parseLong(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("TeraGenWithCRC");
    LOG.info("Running Teragen with CRC");
    job.setJarByClass(TeraGenWithCRC.class);
    job.setMapperClass(SortGenMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(RangeInputFormat.class);
    job.setOutputFormatClass(TeraOutputFormatWithCRC.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraGenWithCRC(), args);
    System.exit(res);
  }

}
