package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.AddDebugAppResponse;;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AddDebugAppResponseProto;

import com.google.protobuf.TextFormat;

public class AddDebugAppResponsePBImpl extends AddDebugAppResponse {
    AddDebugAppResponseProto proto = AddDebugAppResponseProto.getDefaultInstance();
    AddDebugAppResponseProto.Builder builder = null;
    boolean viaProto = false;

    public AddDebugAppResponsePBImpl() {
        builder = AddDebugAppResponseProto.newBuilder();
    }

    public AddDebugAppResponsePBImpl(AddDebugAppResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public AddDebugAppResponseProto getProto() {
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
            builder = AddDebugAppResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }
}
