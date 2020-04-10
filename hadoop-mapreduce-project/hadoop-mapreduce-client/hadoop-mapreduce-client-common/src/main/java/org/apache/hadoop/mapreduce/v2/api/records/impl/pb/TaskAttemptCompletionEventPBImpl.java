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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ServiceMetaDataProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventStatusProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

import com.google.protobuf.ByteString;


    
public class TaskAttemptCompletionEventPBImpl extends ProtoBase<TaskAttemptCompletionEventProto> implements TaskAttemptCompletionEvent {
  TaskAttemptCompletionEventProto proto = TaskAttemptCompletionEventProto.getDefaultInstance();
  TaskAttemptCompletionEventProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskAttemptId taskAttemptId = null;
  
  
  public TaskAttemptCompletionEventPBImpl() {
    builder = TaskAttemptCompletionEventProto.newBuilder();
  }

  public TaskAttemptCompletionEventPBImpl(TaskAttemptCompletionEventProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public TaskAttemptCompletionEventProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskAttemptCompletionEventProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getAttemptId() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    this.taskAttemptId = convertFromProtoFormat(p.getAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setAttemptId(TaskAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) 
      builder.clearAttemptId();
    this.taskAttemptId = attemptId;
  }
  @Override
  public TaskAttemptCompletionEventStatus getStatus() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getStatus());
  }

  @Override
  public void setStatus(TaskAttemptCompletionEventStatus status) {
    maybeInitBuilder();
    if (status == null) {
      builder.clearStatus();
      return;
    }
    builder.setStatus(convertToProtoFormat(status));
  }
  @Override
  public String getMapOutputServerAddress() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMapOutputServerAddress()) {
      return null;
    }
    return (p.getMapOutputServerAddress());
  }

  @Override
  public void setMapOutputServerAddress(String mapOutputServerAddress) {
    maybeInitBuilder();
    if (mapOutputServerAddress == null) {
      builder.clearMapOutputServerAddress();
      return;
    }
    builder.setMapOutputServerAddress((mapOutputServerAddress));
  }
  @Override
  public int getAttemptRunTime() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAttemptRunTime());
  }

  @Override
  public void setAttemptRunTime(int attemptRunTime) {
    maybeInitBuilder();
    builder.setAttemptRunTime((attemptRunTime));
  }
  @Override
  public int getEventId() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getEventId());
  }

  @Override
  public void setEventId(int eventId) {
    maybeInitBuilder();
    builder.setEventId((eventId));
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

  private TaskAttemptCompletionEventStatusProto convertToProtoFormat(TaskAttemptCompletionEventStatus e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private TaskAttemptCompletionEventStatus convertFromProtoFormat(TaskAttemptCompletionEventStatusProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }
  
  
  /*
   * ByteBuffer
   */
  public static ByteBuffer convertGeneralFromProtoFormat(ByteString byteString) {
    int capacity = ((Buffer)byteString.asReadOnlyByteBuffer()).rewind().remaining();
    byte[] b = new byte[capacity];
    byteString.asReadOnlyByteBuffer().get(b, 0, capacity);
    return ByteBuffer.wrap(b);
  }

  public static ByteString convertGeneralToProtoFormat(ByteBuffer byteBuffer) {
    int oldPos = ((Buffer)byteBuffer).position();
    ((Buffer)byteBuffer).rewind();
    ByteString bs = ByteString.copyFrom(byteBuffer);
    ((Buffer)byteBuffer).position(oldPos);
    return bs;
  }


  @Override
  public Map<String, ByteBuffer> getServicesMetaData() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    
    List<ServiceMetaDataProto> servicesList = p.getServiceMetaDataList();
    Map<String, ByteBuffer> result = new HashMap<String, ByteBuffer>(servicesList.size());
    for ( ServiceMetaDataProto proto : servicesList) {
      String name = proto.getServiceName();
      ByteBuffer data = convertGeneralFromProtoFormat(proto.getServiceData());
      result.put(name, data);
    }
    return result;
  }

  @Override
  public void setServicesMetaData(Map<String, ByteBuffer> meta) {
    maybeInitBuilder();
    if ( meta == null || meta.isEmpty() ) {
      builder.clearServiceMetaData();
    } 
    if ( meta != null && !meta.isEmpty() ) {
      for ( Map.Entry<String, ByteBuffer> entry: meta.entrySet()) {
        ServiceMetaDataProto.Builder serviceBuilder = ServiceMetaDataProto.newBuilder();
        serviceBuilder.setServiceName(entry.getKey()).
        setServiceData(convertGeneralToProtoFormat(entry.getValue())).build();
        builder.addServiceMetaData(serviceBuilder);
      }
    }
  }
}  
