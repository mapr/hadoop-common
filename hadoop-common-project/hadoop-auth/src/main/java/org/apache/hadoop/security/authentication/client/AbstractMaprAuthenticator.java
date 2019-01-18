package org.apache.hadoop.security.authentication.client;

import java.net.URL;

public abstract class AbstractMaprAuthenticator implements Authenticator {

  /**
   * HTTP header used by the MAPR server endpoint during an authentication sequence in case of error
   */
  public static final String WWW_ERR_AUTHENTICATE = "WWW-MAPR-Err-Authenticate";

  /**
   * HTTP header prefix used by the MAPR client/server endpoints during an authentication sequence.
   */
  public static final String NEGOTIATE = "MAPR-Negotiate";

  public abstract void authenticate(URL url, AuthenticatedURL.Token token);

  public abstract void setConnectionConfigurator(ConnectionConfigurator configurator);

}

