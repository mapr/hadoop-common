package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class RemoveDebugQueueRequest {
    public static RemoveDebugQueueRequest newInstance() {
      RemoveDebugQueueRequest request =
          Records.newRecord(RemoveDebugQueueRequest.class);
      return request;
    }

    public static RemoveDebugQueueRequest newInstance(String queueName) {
      RemoveDebugQueueRequest request =
          Records.newRecord(RemoveDebugQueueRequest.class);
      request.setQueueName(queueName);
      return request;
    }

    public abstract void setQueueName (String queueName);

    public abstract String getQueueName();

}
