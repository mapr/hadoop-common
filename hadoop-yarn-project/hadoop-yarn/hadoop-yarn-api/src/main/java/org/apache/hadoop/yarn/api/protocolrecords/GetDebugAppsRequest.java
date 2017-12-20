package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public abstract class GetDebugAppsRequest {

    public static GetDebugAppsRequest newInstance() {
      GetDebugAppsRequest request =
          Records.newRecord(GetDebugAppsRequest.class);
      return request;
    }
}
