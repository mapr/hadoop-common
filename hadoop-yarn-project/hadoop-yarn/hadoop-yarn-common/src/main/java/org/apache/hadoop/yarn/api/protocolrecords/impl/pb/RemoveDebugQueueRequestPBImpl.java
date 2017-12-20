package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.RemoveDebugQueueRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugQueueRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugQueueRequestProtoOrBuilder;

public class RemoveDebugQueueRequestPBImpl extends RemoveDebugQueueRequest {

    private String queueName = null;

    RemoveDebugQueueRequestProto proto =
            RemoveDebugQueueRequestProto.getDefaultInstance();
    RemoveDebugQueueRequestProto.Builder builder = null;
    boolean viaProto = false;

    public RemoveDebugQueueRequestPBImpl() {
        builder = RemoveDebugQueueRequestProto.newBuilder();
    }

    public RemoveDebugQueueRequestPBImpl(RemoveDebugQueueRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public RemoveDebugQueueRequestProto getProto() {
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
        if (this.queueName != null) {
            builder.setQueueName(this.queueName);
        }
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = RemoveDebugQueueRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getQueueName() {
        RemoveDebugQueueRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.queueName != null) {
            return this.queueName;
        }
        if (!p.hasQueueName()) {
            return null;
        }
        this.queueName = p.getQueueName();
        return this.queueName;
    }

    @Override
    public void setQueueName(String queueName) {
        maybeInitBuilder();
        if (queueName == null)
            builder.clearQueueName();
        this.queueName = queueName;
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