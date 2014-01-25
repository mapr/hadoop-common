/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.PathId;
import java.util.Arrays;

import org.apache.hadoop.io.WritableUtils;

// mapr_extensibility
public final class PathId {
  private String fid = "";
  private long[] ips = new long[0];


  public String getFid() {
     return fid;
  }


  public long[] getIPs() {
     return ips;
  }

  public void setFid(String fid) {
    this.fid = fid;
  }

  public void setIps(long[] ips) {
    this.ips = ips;
  }


  public void writeFields(DataOutput out) throws IOException {
    WritableUtils.writeString(out, fid);
    WritableUtils.writeVInt(out, ips.length);
    for (long l : ips) {
      WritableUtils.writeVLong(out, l);
    }
  }

  public void readFields(DataInput in) throws IOException {
    fid = WritableUtils.readString(in);
    ips = new long[WritableUtils.readVInt(in)];
    for (int i = 0; i < ips.length; i++) {
      ips[i] = WritableUtils.readVLong(in);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PathId)) {
      return false;
    }
    final PathId otherFileId = (PathId)other;
    return otherFileId.getFid().equals(fid)
        && Arrays.equals(otherFileId.getIPs(), ips);
  }

  @Override
  public String toString() {
    return getClass() + "[ " + fid + " ipaddrs=" + Arrays.toString(ips) + " ]";
  }

}
