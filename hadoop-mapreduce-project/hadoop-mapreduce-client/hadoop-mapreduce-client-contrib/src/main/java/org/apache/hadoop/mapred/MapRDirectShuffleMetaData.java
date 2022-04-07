package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.maprfs.MapRPathId;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

public class MapRDirectShuffleMetaData implements DirectShuffleMetaData {

  private String nmHostName;
  private Map<String, PathId> mrDirPathIds = new HashMap<String, PathId>();

  public void setNodeManageHostName(String hostName) {
    this.nmHostName = hostName;
  }

  public void putDirPathId(String dir, PathId pathId) {
    this.mrDirPathIds.put(dir, pathId);
  }
  
  @Override
  public String getNodeManagerHostName() {
    return nmHostName;
  }

  @Override
  public Map<String, PathId> getMapReduceDirsPathIds() {
    return this.mrDirPathIds;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, nmHostName);
    WritableUtils.writeVInt(out, this.mrDirPathIds.size());
    for (String dirName : mrDirPathIds.keySet()) {
      WritableUtils.writeString(out, dirName);
      mrDirPathIds.get(dirName).writeFields(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.nmHostName = WritableUtils.readString(in);
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      String dirName = WritableUtils.readString(in);
      PathId pathId = new MapRPathId();
      pathId.readFields(in);
      mrDirPathIds.put(dirName, pathId);
    }
  }

}

