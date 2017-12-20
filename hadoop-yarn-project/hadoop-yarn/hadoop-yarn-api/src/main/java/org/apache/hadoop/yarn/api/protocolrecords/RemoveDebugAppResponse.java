package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public class RemoveDebugAppResponse {

    public RemoveDebugAppResponse newInstance () {
      RemoveDebugAppResponse response =
          Records.newRecord(RemoveDebugAppResponse.class);
      return response;
    }

}
