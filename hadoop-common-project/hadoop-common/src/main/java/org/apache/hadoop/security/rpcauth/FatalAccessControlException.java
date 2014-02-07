package org.apache.hadoop.security.rpcauth;

import org.apache.hadoop.security.AccessControlException;

public class FatalAccessControlException extends AccessControlException {

  private static final long serialVersionUID = -2240790088795915455L;

  public FatalAccessControlException() {
    super();
  }

  public FatalAccessControlException(String s) {
    super(s);
  }

  public FatalAccessControlException(Throwable cause) {
    super(cause);
  }

  public FatalAccessControlException(String s, Throwable cause) {
    super(s);
    initCause(cause);
  }

}
