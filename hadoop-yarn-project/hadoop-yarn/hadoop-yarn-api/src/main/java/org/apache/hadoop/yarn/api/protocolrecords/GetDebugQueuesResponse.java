package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

public abstract class GetDebugQueuesResponse {
    public static GetDebugQueuesResponse newInstance() {
      GetDebugQueuesResponse request =
          Records.newRecord(GetDebugQueuesResponse.class);
      return request;
    }

    public static GetDebugQueuesResponse newInstance(Set<String> queues) {
      GetDebugQueuesResponse request =
          Records.newRecord(GetDebugQueuesResponse.class);
      request.setQueues(queues);
      return request;
    }

    public abstract void setQueues (Set<String> queues);

    public abstract Set<String> getQueues();

}
