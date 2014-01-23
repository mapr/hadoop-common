package org.apache.hadoop.mapred;

public interface RequestContextInterface<ReturnType> {

  public void sendResponse(ReturnType response);
  
  public void performRequest();
}
