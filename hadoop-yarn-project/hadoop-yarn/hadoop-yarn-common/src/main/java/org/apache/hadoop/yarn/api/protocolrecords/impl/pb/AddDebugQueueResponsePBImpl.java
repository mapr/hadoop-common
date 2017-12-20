package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.AddDebugQueueResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AddDebugQueueResponseProto;

public class AddDebugQueueResponsePBImpl extends AddDebugQueueResponse {
    AddDebugQueueResponseProto proto = AddDebugQueueResponseProto.getDefaultInstance();
    AddDebugQueueResponseProto.Builder builder = null;
    boolean viaProto = false;

    public AddDebugQueueResponsePBImpl() {
        builder = AddDebugQueueResponseProto.newBuilder();
    }

    public AddDebugQueueResponsePBImpl(AddDebugQueueResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public AddDebugQueueResponseProto getProto() {
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

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = AddDebugQueueResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }
}