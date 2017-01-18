package org.apache.hadoop.io;

import java.io.IOException;

/**
 * Signals that expected user doesn't match real user
 * 
 */
public class PermissionNotMatchException extends IOException {
  private static final long serialVersionUID = 1L;

  public PermissionNotMatchException(String msg) {
        super(msg);
    }
    
}
