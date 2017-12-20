package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

public class AddDebugQueueResponse {

    public AddDebugQueueResponse newInstance () {
      AddDebugQueueResponse response =
          Records.newRecord(AddDebugQueueResponse.class);
      return response;
    }

}
