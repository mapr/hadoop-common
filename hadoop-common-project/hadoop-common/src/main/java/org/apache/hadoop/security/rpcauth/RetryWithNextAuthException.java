package org.apache.hadoop.security.rpcauth;

import java.io.IOException;

public class RetryWithNextAuthException extends IOException {

  private static final long serialVersionUID = -4807670209808616208L;

  private RpcAuthMethod authMethod;

  public RetryWithNextAuthException(RpcAuthMethod authMethod) {
    this.authMethod = authMethod;
  }

  public RpcAuthMethod getAuthMethod() {
    return authMethod;
  }
}
