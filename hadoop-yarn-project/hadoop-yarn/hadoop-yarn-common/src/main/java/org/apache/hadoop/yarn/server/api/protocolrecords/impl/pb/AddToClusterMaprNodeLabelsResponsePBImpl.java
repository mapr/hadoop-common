package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterMaprNodeLabelsResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterMaprNodeLabelsResponse;

public class AddToClusterMaprNodeLabelsResponsePBImpl extends AddToClusterMaprNodeLabelsResponse {

  AddToClusterMaprNodeLabelsResponseProto proto = AddToClusterMaprNodeLabelsResponseProto
      .getDefaultInstance();
  AddToClusterMaprNodeLabelsResponseProto.Builder builder = null;
  boolean viaProto = false;

  public AddToClusterMaprNodeLabelsResponsePBImpl() {
    builder = AddToClusterMaprNodeLabelsResponseProto.newBuilder();
  }

  public AddToClusterMaprNodeLabelsResponsePBImpl(
      AddToClusterMaprNodeLabelsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public AddToClusterMaprNodeLabelsResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}
