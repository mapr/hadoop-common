package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetDebugAppsRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugAppsRequestProto;

public class GetDebugAppsRequestPBImpl extends GetDebugAppsRequest {
    GetDebugAppsRequestProto proto = GetDebugAppsRequestProto.getDefaultInstance();
    GetDebugAppsRequestProto.Builder builder = null;
    boolean viaProto = false;

    public GetDebugAppsRequestPBImpl() {
        builder = GetDebugAppsRequestProto.newBuilder();
    }

    public GetDebugAppsRequestPBImpl(GetDebugAppsRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public GetDebugAppsRequestProto getProto() {
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
            builder = GetDebugAppsRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }
}
