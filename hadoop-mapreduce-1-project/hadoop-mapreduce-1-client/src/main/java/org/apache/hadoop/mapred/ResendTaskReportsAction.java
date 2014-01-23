package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ResendTaskReportsAction extends TaskTrackerAction {
  public ResendTaskReportsAction() {
    super(ActionType.RESEND_STATUS);
  }
  public void write(DataOutput out) throws IOException {}
  public void readFields(DataInput in) throws IOException {}
}
