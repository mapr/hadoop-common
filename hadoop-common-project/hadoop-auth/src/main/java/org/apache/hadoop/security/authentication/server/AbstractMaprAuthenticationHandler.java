package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.AbstractMaprAuthenticator;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;


public abstract class AbstractMaprAuthenticationHandler extends MultiMechsAuthenticationHandler {
  private static Logger LOG = LoggerFactory.
      getLogger(AbstractMaprAuthenticationHandler.class);

  private final String ticketGenerationClass = "com.mapr.security.ClusterServerTicketGeneration";

  /**
   * Authentication type will be embedded in the authentication token
   */
  public static final String TYPE = "maprauth";

/*
  @Override
    public String getType() {
        return TYPE;
    }
*/

  /**
   * This function is invoked when the filter is coming up.
   * we try to get the mapr serverkey which will be used later
   * to decrypt information sent by the client
   * <p>
   * Also since we may be required to authenticate using Kerberos
   * we invoke the kerberos init code after checking if the
   * principal and keytab specified in the config file exist. If they
   * don't exist we don't invoke the kerberos init code because
   * we don't expect to use kerberos.
   *
   * @param config configuration properties to initialize the handler.
   * @throws ServletException
   */
  @Override
  public abstract void init(Properties config) throws ServletException;

  public abstract AuthenticationToken maprAuthenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException;

  @Override
  public void destroy() {
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request,
                                              final HttpServletResponse response)
      throws IOException, AuthenticationException {
    if (request.getHeader(KerberosAuthenticator.AUTHORIZATION) != null) {
      return maprAuthenticate(request, response);
    }
    return null;
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, AbstractMaprAuthenticator.NEGOTIATE);
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if (authorization != null && authorization.startsWith(AbstractMaprAuthenticator.NEGOTIATE)) {
      return this;
    }
    return null;
  }

}
