/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;
package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.List;
import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToLabelsListProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToLabelsListProtoOrBuilder;
import com.google.protobuf.TextFormat;

public class NodeToLabelsListPBImpl extends NodeToLabelsList {

  NodeToLabelsListProto proto = NodeToLabelsListProto.getDefaultInstance();
  NodeToLabelsListProto.Builder builder = null;
  boolean viaProto = false;

  private String node = null;
  private List<String> nodeLabels = null;

  public NodeToLabelsListPBImpl() {
    builder = NodeToLabelsListProto.newBuilder();
  }

  public NodeToLabelsListPBImpl(NodeToLabelsListProto proto) {
    this.proto = proto;
    this.node = proto.getNode();
    viaProto = true;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

   @Override
  public String getNode() {
    if (this.node != null) {
      return this.node;
    }
    NodeToLabelsListProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNode()) {
      return null;
    }
    this.node = p.getNode();
    return this.node;
  }

  @Override
  public List<String> getNodeLabel() {
    if (this.nodeLabels != null) {
      return this.nodeLabels;
    }
	NodeToLabelsListProtoOrBuilder p = viaProto ? proto : builder;
    this.nodeLabels = p.getNodeLabelsList();
    return this.nodeLabels;
  }

  @Override
  public void setNode(String node) {
    maybeInitBuilder();
    if (node == null) {
      builder.clearNode();
    }
    this.node = node;
  }

  @Override
  public void setNodeLabel(List<String> nodeLabels) {
    maybeInitBuilder();
    if(nodeLabels == null) {
      builder.clearNodeLabels();
    }
    this.nodeLabels = nodeLabels;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeToLabelsListProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public NodeToLabelsListProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    builder.setNode(this.node);
    if (this.nodeLabels != null
      && !(nodeLabels.equals(builder.getNodeLabelsList()))) {
      builder.clearNodeLabels();
      builder.addAllNodeLabels(this.nodeLabels);
    }
  }

  @Override
  public int hashCode() {
    return this.getProto().hashCode();
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
}
