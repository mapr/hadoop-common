package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterMaprNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterMaprNodeLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterMaprNodeLabelsRequest;

public class AddToClusterMaprNodeLabelsRequestPBImpl extends AddToClusterMaprNodeLabelsRequest {

  String labels;
  AddToClusterMaprNodeLabelsRequestProto proto = AddToClusterMaprNodeLabelsRequestProto
      .getDefaultInstance();
  AddToClusterMaprNodeLabelsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public AddToClusterMaprNodeLabelsRequestPBImpl() {
    this.builder = AddToClusterMaprNodeLabelsRequestProto.newBuilder();
  }

  public AddToClusterMaprNodeLabelsRequestPBImpl(
      AddToClusterMaprNodeLabelsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddToClusterMaprNodeLabelsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.labels != null && !this.labels.isEmpty()) {
      builder.clearNodeLabels();
      builder.setNodeLabels(this.labels);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public AddToClusterMaprNodeLabelsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void initLabels() {
    if (this.labels != null) {
      return;
    }
    AddToClusterMaprNodeLabelsRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.labels = p.getNodeLabels();
  }

  @Override
  public void setNodeLabels(String labels) {
    maybeInitBuilder();
    if (labels == null || labels.isEmpty()) {
      builder.clearNodeLabels();
    }
    this.labels = labels;
  }

  @Override
  public String getNodeLabels() {
    initLabels();
    return this.labels;
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

}
