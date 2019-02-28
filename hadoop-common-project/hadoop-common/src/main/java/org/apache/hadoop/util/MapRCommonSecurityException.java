package org.apache.hadoop.util;

import java.io.IOException;

public class MapRCommonSecurityException extends IOException {

  public MapRCommonSecurityException(String msg) {
    super(msg);
  }

  public MapRCommonSecurityException(String msg, Throwable t) {
    super(msg, t);
  }

  public MapRCommonSecurityException(Throwable t) {
    super(t);
  }
}
