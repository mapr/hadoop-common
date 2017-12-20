package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public class RemoveDebugQueueResponse {

    public RemoveDebugQueueResponse newInstance () {
      RemoveDebugQueueResponse response =
          Records.newRecord(RemoveDebugQueueResponse.class);
      return response;
    }

}
