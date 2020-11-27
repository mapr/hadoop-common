package org.apache.hadoop.security.authentication.server;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthHandler extends MultiMechsAuthenticationHandler {

  private static Logger LOG = LoggerFactory.getLogger(BasicAuthHandler.class);
  private static final String BASIC_AUTH = "Basic";
  private final String passwordAuthenticationClass = "com.mapr.login.PasswordAuthentication";

  @Override
  public void init(Properties config) throws ServletException {
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request,
                                              final HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken authToken = null;
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    if (authorization != null && authorization.startsWith(BASIC_AUTH)) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Credentials: " + authorization);
        }
        authorization = authorization.substring(authorization.indexOf(' ') + 1);
        authorization = new String(Base64.decodeBase64(authorization));
        int i = authorization.indexOf(':');
        String username = authorization.substring(0, i);
        String password = authorization.substring(i + 1);
        Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(passwordAuthenticationClass);
        Method passAuth = klass.getDeclaredMethod("authenticate", String.class, String.class);
        if ((boolean) passAuth.invoke(null, username, password)) {
          authToken = new AuthenticationToken(username, username, getType());
          response.setStatus(HttpServletResponse.SC_OK);
        } else {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          LOG.error("User Principal is null while trying to authenticate with Basic Auth");
        }
      } catch (Exception e) {
        LOG.warn("AUTH FAILURE: " + e.toString());
      }
    }

    return authToken;
  }

  /*
    @Override
    public String getType() {
      return "basic";
    }
  */
  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Basic realm=\"" + "WebLogin" + '"');
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if (authorization != null && authorization.startsWith(BASIC_AUTH)) {
      return this;
    }
    return null;
  }

}
