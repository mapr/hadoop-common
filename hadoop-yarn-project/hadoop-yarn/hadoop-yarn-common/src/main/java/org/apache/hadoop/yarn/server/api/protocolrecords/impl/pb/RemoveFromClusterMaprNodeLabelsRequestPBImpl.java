/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterMaprNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterMaprNodeLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterMaprNodeLabelsRequest;

public class RemoveFromClusterMaprNodeLabelsRequestPBImpl extends
    RemoveFromClusterMaprNodeLabelsRequest {

  String labels;
  RemoveFromClusterMaprNodeLabelsRequestProto proto =
      RemoveFromClusterMaprNodeLabelsRequestProto.getDefaultInstance();
  RemoveFromClusterMaprNodeLabelsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public RemoveFromClusterMaprNodeLabelsRequestPBImpl() {
    this.builder = RemoveFromClusterMaprNodeLabelsRequestProto.newBuilder();
  }

  public RemoveFromClusterMaprNodeLabelsRequestPBImpl(
      RemoveFromClusterMaprNodeLabelsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RemoveFromClusterMaprNodeLabelsRequestProto.newBuilder(proto);
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

  public RemoveFromClusterMaprNodeLabelsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void initNodeLabels() {
    if (this.labels != null) {
      return;
    }
    RemoveFromClusterMaprNodeLabelsRequestProtoOrBuilder p =
        viaProto ? proto : builder;

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
    initNodeLabels();
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
