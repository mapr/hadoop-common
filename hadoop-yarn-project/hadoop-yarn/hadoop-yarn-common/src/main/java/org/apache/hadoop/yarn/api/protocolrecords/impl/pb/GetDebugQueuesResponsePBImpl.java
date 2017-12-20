package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetDebugQueuesResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugQueuesResponseProtoOrBuilder;

import java.util.HashSet;
import java.util.Set;

public class GetDebugQueuesResponsePBImpl extends GetDebugQueuesResponse {

    private Set<String> queues = null;

    GetDebugQueuesResponseProto proto =
            GetDebugQueuesResponseProto.getDefaultInstance();
    GetDebugQueuesResponseProto.Builder builder = null;
    boolean viaProto = false;

    public GetDebugQueuesResponsePBImpl() {
        builder = GetDebugQueuesResponseProto.newBuilder();
    }

    public GetDebugQueuesResponsePBImpl(GetDebugQueuesResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public GetDebugQueuesResponseProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void mergeLocalToBuilder() {
        if (this.queues != null && !this.queues.isEmpty()) {
            builder.clearQueues();
            builder.addAllQueues(this.queues);
        }
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = GetDebugQueuesResponseProto.newBuilder(proto);
        }
        viaProto = false;
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

    private void initQueues() {

        if (this.queues != null) {
            return;
        }
        GetDebugQueuesResponseProtoOrBuilder p = viaProto ? proto : builder;
        this.queues = new HashSet<>();
        this.queues.addAll(p.getQueuesList());
    }

    @Override
    public void setQueues(Set<String> queues) {
        maybeInitBuilder();
        if (queues == null || queues.isEmpty()) {
            builder.clearQueues();
        }
        this.queues = queues;
    }

    @Override
    public Set<String> getQueues() {
        initQueues();
        return this.queues;
    }
}