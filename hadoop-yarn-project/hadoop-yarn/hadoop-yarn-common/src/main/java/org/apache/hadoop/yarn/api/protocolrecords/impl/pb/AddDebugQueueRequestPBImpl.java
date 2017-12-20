package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.AddDebugQueueRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AddDebugQueueRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AddDebugQueueRequestProtoOrBuilder;

public class AddDebugQueueRequestPBImpl extends AddDebugQueueRequest {

    private String queueName = null;

    AddDebugQueueRequestProto proto =
            AddDebugQueueRequestProto.getDefaultInstance();
    AddDebugQueueRequestProto.Builder builder = null;
    boolean viaProto = false;

    public AddDebugQueueRequestPBImpl() {
        builder = AddDebugQueueRequestProto.newBuilder();
    }

    public AddDebugQueueRequestPBImpl(AddDebugQueueRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public AddDebugQueueRequestProto getProto() {
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
            builder = AddDebugQueueRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getQueueName() {
        AddDebugQueueRequestProtoOrBuilder p = viaProto ? proto : builder;
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