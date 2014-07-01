package org.apache.hadoop.security.authentication.server;

/**
 * Author: smarella
 */
public interface CookieSignatureSecretFactory {
  public String getSignatureSecret();
}
