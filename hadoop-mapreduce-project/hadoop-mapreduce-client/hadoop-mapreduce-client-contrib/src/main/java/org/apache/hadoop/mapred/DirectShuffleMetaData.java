package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.Writable;

import java.util.Map;

/**
*
* The metadata exposed by the direct shuffle auxiliary
* service running inside node manager.
* 
* 
* TODO(Santosh): Move this interface into mapreduce contrib on GIT.
*
*/
public interface DirectShuffleMetaData extends Writable {
  public static final String DIRECT_SHUFFLE_SERVICE_ID = "direct_shuffle";

  public String getNodeManagerHostName();

  public Map<String, PathId> getMapReduceDirsPathIds();

}

