package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetDebugQueuesRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugQueuesRequestProto;

public class GetDebugQueuesRequestPBImpl extends GetDebugQueuesRequest {
    GetDebugQueuesRequestProto proto = GetDebugQueuesRequestProto.getDefaultInstance();
    GetDebugQueuesRequestProto.Builder builder = null;
    boolean viaProto = false;

    public GetDebugQueuesRequestPBImpl() {
        builder = GetDebugQueuesRequestProto.newBuilder();
    }

    public GetDebugQueuesRequestPBImpl(GetDebugQueuesRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public GetDebugQueuesRequestProto getProto() {
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
            builder = GetDebugQueuesRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }
}
