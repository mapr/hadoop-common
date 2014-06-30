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
package org.apache.hadoop.examples.blocklocality;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Launches the job, and waits for it to finish.
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar blocklocality in-dir out-dir</b>
 */
public class BlockLocality extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(BlockLocality.class);

  static class BlockLocalityMapper extends MapReduceBase
    implements Mapper<Text, Text, Text, IntWritable> {

    private JobConf conf;
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);

    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void map(Text key, Text value, 
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String hostname = null;
      try{
//        InetAddress addr = InetAddress.getLocalHost();
        //byte[] ipAddr = addr.getAddress();
//        hostname = addr.getHostName();
        FileInputStream fstream = new FileInputStream("/opt/mapr/hostname");
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        hostname = br.readLine();
        in.close();
//        key = new Text(hostname);
      } catch (IOException e) {
        //do something with the exception
        return;
      }
      
      System.out.println("hostname = " + hostname);
      System.out.println(value.toString());
      String[] blockLocations = value.toString().split(";");
      for (int i = 0; i < blockLocations.length; i++) {
        System.out.println("Block located at: " + blockLocations[i]);
        if (hostname.compareTo(blockLocations[i]) == 0) {
          output.collect(new Text(hostname), one);
          return;
        }
        if (hostname.compareTo(blockLocations[i].split(".",1)[0]) == 0) {
          output.collect(new Text(hostname), one);
          return;
        }
        if (hostname.split(".",1)[0].compareTo(blockLocations[i]) == 0) {
          output.collect(new Text(hostname), one);
          return;
        }
      }
      output.collect(new Text(hostname), zero);
//      output.collect(value, one);
    }
  } 
  static class BlockLocalityReducer extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      int count = 0;
      int sum = 0;
      while (values.hasNext()) {
        count++;
        sum += values.next().get();
      }
      output.collect(new Text(key.toString() + " = "), new Text(String.valueOf((float)sum/(float)count)));
    }
  }

  public int run(String[] args) throws Exception {
    LOG.info("starting");
    JobConf job = (JobConf) getConf();
    Path inputDir = new Path(args[0]);
    inputDir = inputDir.makeQualified(inputDir.getFileSystem(job));

    LocalityInputFormat.setInputPaths(job, new Path(args[0]));
    LocalityOutputFormat.setOutputPath(job, new Path(args[1]));
    LocalityOutputFormat.setFinalSync(job, true);

    job.setJobName("BlockLocality");
    job.setJarByClass(BlockLocality.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormat(LocalityInputFormat.class);
    job.setOutputFormat(LocalityOutputFormat.class);

    job.setMapperClass(BlockLocalityMapper.class);
    job.setReducerClass(BlockLocalityReducer.class);

    job.set("mapred.reduce.tasks", "1");

    JobClient.runJob(job);
    LOG.info("done");
    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new BlockLocality(), args);
    System.exit(res);
  }
}
