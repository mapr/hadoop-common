package org.apache.hadoop.yarn.server.nodemanager.containermanager.nodelocallogaggregation;

import org.apache.hadoop.yarn.api.records.ContainerId;

import java.util.ArrayList;
import java.util.List;

public class NodeLocalApplicationLogMetadata {
  private String applicationId;
  private String appOwner;
  private String nodeId;
  private List<ContainerId> containers;

  public NodeLocalApplicationLogMetadata() {
    containers = new ArrayList<>();
  }

  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public String getAppOwner() {
    return appOwner;
  }

  public void setAppOwner(String appOwner) {
    this.appOwner = appOwner;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public List<ContainerId> getContainers() {
    return containers;
  }

  public void setContainers(List<ContainerId> containers) {
    this.containers = containers;
  }
}
