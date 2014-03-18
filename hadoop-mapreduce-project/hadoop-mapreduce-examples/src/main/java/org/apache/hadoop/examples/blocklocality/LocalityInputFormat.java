/**
 */

package org.apache.hadoop.examples.blocklocality;

import java.io.IOException;
import java.util.ArrayList;
import java.net.InetAddress;
import java.lang.StringBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

//import com.mapr.fs.*;

/**
 * An input format that reads block location information from input file
 * and returns 1 as key and block location information as value.
 * Both key and value are represented as Text.
 */
public class LocalityInputFormat extends FileInputFormat<Text, Text> {

  public RecordReader<Text, Text> getRecordReader(
    InputSplit split, JobConf job, Reporter reporter) throws IOException {

    return new LocalityRecordReader((FileSplit) split, job);
  }

  static class LocalityRecordReader implements RecordReader<Text, Text> {
    private FileSplit fileSplit;
    private Configuration conf;
    private FileSystem fs;
    private FSDataInputStream in;
    private boolean processed = false;
    private FileStatus fstatus;
    private Path file;

    public LocalityRecordReader(FileSplit fileSplit, Configuration conf) 
                            throws IOException {
      this.fileSplit = fileSplit;
      long offset = fileSplit.getStart();
      this.conf = conf;
      file = fileSplit.getPath();
      fs = file.getFileSystem(conf);
      try {
        in = fs.open(file);
        in.seek(offset);
        System.out.println("fileSplit offset = " + offset);
        System.out.println("Current position after seek to offset = " + in.getPos());
      } finally {
      //do nothing
      }
    }

    public void close() throws IOException {
      in.close();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public float getProgress() throws IOException {
      return processed ? 1.0f : 0.0f;
    }

    public boolean next(Text key, Text value) throws IOException {
      if (processed) {
        return false;
      }

      char[] data = new char[2000];
      char tempc;
      try {
        while (in.readChar() != '<') {
          //do nothing
        }

        fstatus = fs.getFileStatus(file);
        BlockLocation[] bloc = fs.getFileBlockLocations(fstatus, in.getPos(), in.getPos()+10L);
      if (bloc == null) {
        System.out.println("null ********");
        System.exit(-1);
      } else {
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < (bloc.length); ++j) {
          String[] hostnames = bloc[j].getHosts();
          for (int k = 0; k < hostnames.length; k++) {
            sb.append(hostnames[k]);
            sb.append(";");
          }
        }
        System.out.println("Starting position is " + in.getPos());

/*        for (int i = 0; (tempc=in.readChar()) != '>'; i++) {
          data[i] = tempc;
        }
        String s = new String(data);
        System.out.println("value is set to: " + s);
        System.out.println("Ending position is " + in.getPos());
*/

        key.set("1");
        value.set(sb.toString()); 
      }
      } finally {
        processed = true;
        return true;
      }
    }
  } //LocalityRecordReader class
}
