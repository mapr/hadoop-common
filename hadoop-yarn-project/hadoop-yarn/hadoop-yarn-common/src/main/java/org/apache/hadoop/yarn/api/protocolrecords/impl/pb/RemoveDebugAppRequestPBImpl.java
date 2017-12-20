package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.RemoveDebugAppRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugAppRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RemoveDebugAppRequestProtoOrBuilder;

public class RemoveDebugAppRequestPBImpl extends RemoveDebugAppRequest {

    private String applicationId = null;

    RemoveDebugAppRequestProto proto =
            RemoveDebugAppRequestProto.getDefaultInstance();
    RemoveDebugAppRequestProto.Builder builder = null;
    boolean viaProto = false;

    public RemoveDebugAppRequestPBImpl() {
        builder = RemoveDebugAppRequestProto.newBuilder();
    }

    public RemoveDebugAppRequestPBImpl(RemoveDebugAppRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public RemoveDebugAppRequestProto getProto() {
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
        if (this.applicationId != null) {
            builder.setApplicationId(this.applicationId);
        }
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = RemoveDebugAppRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getApplicationId() {
        RemoveDebugAppRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.applicationId != null) {
            return this.applicationId;
        }
        if (!p.hasApplicationId()) {
            return null;
        }
        this.applicationId = p.getApplicationId();
        return this.applicationId;
    }

    @Override
    public void setApplicationId(String applicationId) {
        maybeInitBuilder();
        if (applicationId == null)
            builder.clearApplicationId();
        this.applicationId = applicationId;
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
