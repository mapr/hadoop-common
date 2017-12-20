package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class RemoveDebugAppRequest {

    public static RemoveDebugAppRequest newInstance() {
      RemoveDebugAppRequest request =
          Records.newRecord(RemoveDebugAppRequest.class);
      return request;
    }

    public static RemoveDebugAppRequest newInstance(String applicationId) {
      RemoveDebugAppRequest request =
          Records.newRecord(RemoveDebugAppRequest.class);
      request.setApplicationId(applicationId);
      return request;
    }

    public abstract void setApplicationId (String applicationId);

    public abstract String getApplicationId();

}
