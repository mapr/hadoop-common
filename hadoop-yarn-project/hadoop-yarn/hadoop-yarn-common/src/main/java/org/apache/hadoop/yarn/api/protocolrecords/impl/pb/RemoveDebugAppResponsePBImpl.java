package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.RemoveDebugAppResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugAppResponseProto;

public class RemoveDebugAppResponsePBImpl extends RemoveDebugAppResponse {
    RemoveDebugAppResponseProto proto = RemoveDebugAppResponseProto.getDefaultInstance();
    RemoveDebugAppResponseProto.Builder builder = null;
    boolean viaProto = false;

    public RemoveDebugAppResponsePBImpl() {
        builder = RemoveDebugAppResponseProto.newBuilder();
    }

    public RemoveDebugAppResponsePBImpl(RemoveDebugAppResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public RemoveDebugAppResponseProto getProto() {
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
            builder = RemoveDebugAppResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

}
