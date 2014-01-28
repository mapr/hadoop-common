package org.apache.hadoop.mapred;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;

public class LocalVolumeHandlingService extends AuxiliaryService {

  protected LocalVolumeHandlingService(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void initializeApplication(
      ApplicationInitializationContext initAppContext) {
    // TODO Auto-generated method stub

  }

  @Override
  public void stopApplication(ApplicationTerminationContext stopAppContext) {
    // TODO Auto-generated method stub

  }

  @Override
  public ByteBuffer getMetaData() {
    // TODO Auto-generated method stub
    return null;
  }

}
