package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

public abstract class GetDebugAppsResponse {
    public static GetDebugAppsResponse newInstance() {
      GetDebugAppsResponse request =
          Records.newRecord(GetDebugAppsResponse.class);
      return request;
    }

    public static GetDebugAppsResponse newInstance(Set<String> applications) {
      GetDebugAppsResponse request =
          Records.newRecord(GetDebugAppsResponse.class);
      request.setApplications(applications);
      return request;
    }

    public abstract void setApplications (Set<String> applications);

    public abstract Set<String> getApplications();

}
