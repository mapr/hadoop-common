package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class AddToClusterMaprNodeLabelsRequest {

  public static AddToClusterMaprNodeLabelsRequest newInstance(String labels) {
    AddToClusterMaprNodeLabelsRequest request =
        Records.newRecord(AddToClusterMaprNodeLabelsRequest.class);
    request.setNodeLabels(labels);
    return request;
  }

  @Public
  @Evolving
  public abstract void setNodeLabels(String labels);

  @Public
  @Evolving
  public abstract String getNodeLabels();

}
