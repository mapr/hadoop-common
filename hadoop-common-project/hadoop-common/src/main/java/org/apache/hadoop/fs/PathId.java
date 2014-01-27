/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.PathId;


public interface PathId {

  public String getFid();

  public long[] getIPs();
  
  public void setFid(String fid);
  
  public void addIp(long ip);
  
  public void setIps(List<Long> ips);

  public void writeFields(DataOutput out) throws IOException;

  public void readFields(DataInput in) throws IOException;
}
