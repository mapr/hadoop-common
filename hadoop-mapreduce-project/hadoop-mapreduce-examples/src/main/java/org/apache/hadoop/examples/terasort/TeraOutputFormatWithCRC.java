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
import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class TeraOutputFormatWithCRC extends TextOutputFormat<Text,Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobContext conf, boolean newValue) {
    conf.getConfiguration().setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobContext conf) {
    return conf.getConfiguration().getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter extends RecordWriter<Text,Text> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;
    private String space = "";
    private int CRC_LEN = 20;

    private FSDataOutputStream out;

    public TeraRecordWriter(FSDataOutputStream out,
                            JobContext conf) {
      this.out = out;
      finalSync = getFinalSync(conf);
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
     	out.write(key.getBytes(), 0, key.getLength());
      
      StringBuilder spaces = new StringBuilder(CRC_LEN);
      byte[] valueToCksum = value.copyBytes();
    	CRC32 checksum = new CRC32();
    	checksum.update(key.getBytes());
    	System.out.println("key checksum: " + checksum.getValue());
    	checksum.update(valueToCksum,0,valueToCksum.length);
    	System.out.println("total checksum: " + checksum.getValue());
    	long cksum = checksum.getValue();
    	byte[] cksum_bytes = Long.toString(cksum).getBytes();
    	int padspaces = CRC_LEN - cksum_bytes.length;
    	if(padspaces > 0)
    	{
    		for(int i = 0;i < padspaces; ++i){
        	spaces.append(' ');
        }
    		value.append(spaces.toString().getBytes(), 0, spaces.toString().getBytes().length);
    	}
     	value.append(cksum_bytes, 0, cksum_bytes.length);
    	out.write(value.getBytes(), 0, value.getLength());
    }
    
    public void close(TaskAttemptContext context) throws IOException {
        if (finalSync) {
          out.sync();
        }
        out.close();
      }
  }

  public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext job) 
  			throws IOException {
	  Path file = getDefaultWorkFile(job, "");
	  FileSystem fs = file.getFileSystem(job.getConfiguration());
	  FSDataOutputStream fileOut = fs.create(file);
	  return new TeraRecordWriter(fileOut, job);
  }
  
}
