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

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraValidateRecords extends Configured 
																 implements Tool {
	
	static Log LOG = LogFactory.getLog(TeraValidateRecords.class);
	
	static class TeraValidateMapper extends MapReduceBase 
			implements Mapper<Text,Text,IntWritable,LongWritable> {
		private static int ROW_LENGTH = 10;
		private int numPartitions;
		private long num_records;
		private long num_records_per_partitioner;
		
		public void configure(JobConf conf){
			numPartitions = conf.getNumReduceTasks();
			num_records = conf.getLong("map.num.records",1);
			num_records_per_partitioner  = num_records/numPartitions;
		}
		
		public void map(Text key, Text value, 
				OutputCollector<IntWritable, LongWritable> output,
        Reporter reporter) throws IOException {
	    String rowid_str = new String(value.getBytes(),0,ROW_LENGTH).trim();
	    long rowid = Long.parseLong(rowid_str);
	    int partition_num = (int)(rowid/num_records_per_partitioner);
	    LOG.info("Partition number for rowid " + rowid + " " + partition_num);
	    output.collect(new IntWritable(partition_num), new LongWritable(rowid));
    }
	}
	
	static class TeraValidateReducer extends MapReduceBase 
		implements Reducer<IntWritable, LongWritable, LongWritable, Text> {
		
		private int numPartitions;
		private long num_records;
		private long num_records_per_partitioner;
                private TreeSet<Long> records_hash = null; 
		
		public void configure(JobConf conf){
			numPartitions = conf.getNumReduceTasks();
			num_records = conf.getLong("map.num.records",1);
			num_records_per_partitioner  = num_records/numPartitions;
		}
		
	  public void reduce(IntWritable key, Iterator<LongWritable> values,
        OutputCollector<LongWritable, Text> output, 
        Reporter reporter) throws IOException {
	  	
  	  long rowid = 0;
  
  		records_hash = new TreeSet<Long>();
    	while(values.hasNext()){
    		rowid = values.next().get();
      	if(records_hash.contains(rowid)){
    		  output.collect(new LongWritable(rowid),new Text("Duplicate"));
    		}  
    		else{
    		  records_hash.add(rowid);
  		  }
  	  }
  		
      long startRowId = key.get() * num_records_per_partitioner;
  		
  		for(rowid = startRowId; rowid < (startRowId + num_records_per_partitioner);++rowid){
  			if(!records_hash.contains(rowid))
  				output.collect(new LongWritable(rowid), new Text("Missing"));
  		}
  	}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = (JobConf) getConf();
		int num_records = 0;
		int num_reducers = 1;
		String inputPath = null;
		String outputPath = null;
		
		if(args.length >= 4)
		{
			num_records = Integer.parseInt(args[0]);
			num_reducers = Integer.parseInt(args[1]);
			inputPath = args[2];
			outputPath = args[3];
		}
		else
		{
			LOG.error("Usage: teravalidaterecords <number of records> <number of reducers>" +
					"<input> <output>");
			System.exit(1);
		}
		
		conf.setLong("map.num.records", num_records);
		
		TeraInputFormatWithCRC.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setJobName("TeraValidateRecords");
		
		conf.setJarByClass(TeraValidateRecords.class);
		
		conf.setMapperClass(TeraValidateMapper.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		
		conf.setMapOutputValueClass(LongWritable.class);
		
		conf.setReducerClass(TeraValidateReducer.class);
		
		conf.setNumReduceTasks(num_reducers);
		
		conf.setOutputKeyClass(LongWritable.class);
		
		conf.setOutputValueClass(Text.class);
		
    conf.setInputFormat(TeraInputFormatWithCRC.class);
    
    conf.setOutputFormat(TextOutputFormat.class);
    
    JobClient.runJob(conf);
		return 0;
  }
	
	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraValidateRecords(), args);
    System.exit(res);
  }
	
}
