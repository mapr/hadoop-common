package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.PathId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MapRPathId implements PathId {
  @Override
  public String getFid() {
    return null;
  }

  @Override
  public long[] getIPs() {
    return new long[0];
  }

  @Override
  public void setFid(String fid) {

  }

  @Override
  public void setIps(long[] ips) {

  }

  @Override
  public void setIps(List<Long> ips) {

  }

  @Override
  public void writeFields(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }
}
