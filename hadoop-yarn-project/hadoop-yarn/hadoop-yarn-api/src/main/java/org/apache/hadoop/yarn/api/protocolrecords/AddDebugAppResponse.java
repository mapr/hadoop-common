package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public class AddDebugAppResponse {

    public AddDebugAppResponse newInstance () {
      AddDebugAppResponse response =
          Records.newRecord(AddDebugAppResponse.class);
      return response;
    }

}
