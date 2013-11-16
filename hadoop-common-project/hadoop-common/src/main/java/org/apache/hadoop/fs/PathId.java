/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.PathId;

import org.apache.hadoop.io.WritableUtils;

public interface PathId {

  public String getFid();

  public long[] getIPs();

  public void writeFields(DataOutput out) throws IOException;

  public void readFields(DataInput in) throws IOException;
}
