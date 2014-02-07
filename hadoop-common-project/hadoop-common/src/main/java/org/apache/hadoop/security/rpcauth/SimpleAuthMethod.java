package org.apache.hadoop.security.rpcauth;

import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto.Builder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

public final class SimpleAuthMethod extends RpcAuthMethod {
  static final RpcAuthMethod INSTANCE = new SimpleAuthMethod();
  private SimpleAuthMethod() {
    super((byte) 80, "simple", "", AuthenticationMethod.SIMPLE);
  }

  @Override
  public void writeUGI(UserGroupInformation ugi, Builder ugiProto) {
    ugiProto.setEffectiveUser(ugi.getUserName());
    if (ugi.getRealUser() != null) {
      ugiProto.setRealUser(ugi.getRealUser().getUserName());
    }
  }
}