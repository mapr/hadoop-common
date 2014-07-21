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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class TeraOutputFormatWithCRC extends TextOutputFormat<Text,Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobConf conf, boolean newValue) {
    conf.setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobConf conf) {
    return conf.getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter extends LineRecordWriter<Text,Text> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;
    private String space = "";
    private int CRC_LEN = 20;

    public TeraRecordWriter(DataOutputStream out,
                            JobConf conf) {
      super(out);
      finalSync = getFinalSync(conf);
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
    	out.write(key.getBytes(), 0, key.getLength());
      //out.write(value.getBytes(), 0, value.getLength());
      
      StringBuffer spaces = new StringBuffer(CRC_LEN);
      byte[] valueToCksum = value.toString().getBytes();
    	CRC32 checksum = new CRC32();
    	checksum.update(key.toString().getBytes());
    	checksum.update(valueToCksum,0,valueToCksum.length);
    	long cksum = checksum.getValue();
    	byte[] cksum_bytes = Long.toString(cksum).getBytes();
    	int padspaces = CRC_LEN - cksum_bytes.length;
    	if(padspaces > 0)
    	{
    		for(int i = 0;i < padspaces; ++i){
        	spaces.append(" ");
        }
    		//out.write(spaces.toString().getBytes());
    		value.append(spaces.toString().getBytes(), 0, spaces.toString().getBytes().length);
    	}
    	//out.write(cksum_bytes);
    	value.append(cksum_bytes, 0, cksum_bytes.length);
    	out.write(value.getBytes(), 0, value.getLength());
    	out.write(newLine, 0, newLine.length);
    }
    
    public void close() throws IOException {
      if (finalSync) {
        ((FSDataOutputStream) out).sync();
      }
      super.close(null);
    }
  }

  public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored,
                                                 JobConf job,
                                                 String name,
                                                 Progressable progress
                                                 ) throws IOException {
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new TeraRecordWriter(fileOut, job);
  }
}
