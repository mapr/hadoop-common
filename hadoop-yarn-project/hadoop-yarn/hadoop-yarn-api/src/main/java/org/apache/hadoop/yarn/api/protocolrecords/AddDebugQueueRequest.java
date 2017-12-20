package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class AddDebugQueueRequest {

    public static AddDebugQueueRequest newInstance() {
      AddDebugQueueRequest request =
          Records.newRecord(AddDebugQueueRequest.class);
      return request;
    }

    public static AddDebugQueueRequest newInstance(String queueName) {
      AddDebugQueueRequest request =
          Records.newRecord(AddDebugQueueRequest.class);
      request.setQueueName(queueName);
      return request;
    }

    public abstract void setQueueName (String queueName);

    public abstract String getQueueName();

}
