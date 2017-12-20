package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;


public abstract class AddDebugAppRequest {

    public static AddDebugAppRequest newInstance() {
      AddDebugAppRequest request =
          Records.newRecord(AddDebugAppRequest.class);
      return request;
    }

    public static AddDebugAppRequest newInstance(String applicationId) {
      AddDebugAppRequest request =
          Records.newRecord(AddDebugAppRequest.class);
      request.setApplicationId(applicationId);
      return request;
    }

    public abstract void setApplicationId (String applicationId);

    public abstract String getApplicationId();

}
