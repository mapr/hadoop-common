package org.apache.hadoop.maprfs;

import org.apache.hadoop.fs.PathId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.PathId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

public final class MapRPathId implements PathId {
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

  public void setIps(List<Long> listIps) {
    ips = new long[listIps.size()];
    for ( int i = 0; i < listIps.size(); i++ ) {
      ips[i] = listIps.get(i);
    }
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
    if (!(other instanceof MapRPathId)) {
      return false;
    }
    final MapRPathId otherFileId = (MapRPathId) other;
    return otherFileId.getFid().equals(fid)
        && Arrays.equals(otherFileId.getIPs(), ips);
  }

  @Override
  public String toString() {
    return getClass() + "[ " + fid + " ipaddrs=" + Arrays.toString(ips) + " ]";
  }

}
