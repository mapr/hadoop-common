package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class GetDebugQueuesRequest {

    public static GetDebugQueuesRequest newInstance() {
      GetDebugQueuesRequest request =
          Records.newRecord(GetDebugQueuesRequest.class);
      return request;
    }
}
