package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetDebugAppsResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugAppsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetDebugAppsResponseProto;

import java.util.HashSet;
import java.util.Set;

public class GetDebugAppsResponsePBImpl extends GetDebugAppsResponse {

    private Set<String> applications = null;

    GetDebugAppsResponseProto proto =
            GetDebugAppsResponseProto.getDefaultInstance();
    GetDebugAppsResponseProto.Builder builder = null;
    boolean viaProto = false;

    public GetDebugAppsResponsePBImpl() {
        builder = GetDebugAppsResponseProto.newBuilder();
    }

    public GetDebugAppsResponsePBImpl(GetDebugAppsResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public GetDebugAppsResponseProto getProto() {
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
        if (this.applications != null && !this.applications.isEmpty()) {
            builder.clearApplications();
            builder.addAllApplications(this.applications);
        }
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = GetDebugAppsResponseProto.newBuilder(proto);
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

    private void initApplications() {

        if (this.applications != null) {
            return;
        }
        GetDebugAppsResponseProtoOrBuilder p = viaProto ? proto : builder;
        this.applications = new HashSet<>();
        this.applications.addAll(p.getApplicationsList());
    }

    @Override
    public void setApplications(Set<String> applications) {
        maybeInitBuilder();
        if (applications == null || applications.isEmpty()) {
            builder.clearApplications();
        }
        this.applications = applications;
    }

    @Override
    public Set<String> getApplications() {
        initApplications();
        return this.applications;
    }
}