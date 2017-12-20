package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.RemoveDebugQueueResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugQueueResponseProto;

public class RemoveDebugQueueResponsePBImpl extends RemoveDebugQueueResponse {
    RemoveDebugQueueResponseProto proto = RemoveDebugQueueResponseProto.getDefaultInstance();
    RemoveDebugQueueResponseProto.Builder builder = null;
    boolean viaProto = false;

    public RemoveDebugQueueResponsePBImpl() {
        builder = RemoveDebugQueueResponseProto.newBuilder();
    }

    public RemoveDebugQueueResponsePBImpl(RemoveDebugQueueResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public RemoveDebugQueueResponseProto getProto() {
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
            builder = RemoveDebugQueueResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }
}

