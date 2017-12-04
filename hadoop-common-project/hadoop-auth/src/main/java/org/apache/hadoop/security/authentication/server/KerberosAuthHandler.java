/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.security.authentication.server;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.log4j.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

public class KerberosAuthHandler extends MultiMechsAuthenticationHandler {

  private static final Logger LOG = Logger.getLogger(KerberosAuthHandler.class);

  protected GSSManager gssManager;
  protected LoginContext loginContext;
  protected String principal;
  protected String keytab;


  @Override
  public void init(Properties config) throws ServletException {
    if (loginContext != null)
      return;

    try {
      loginContext = new LoginContext("MAPR_WEBSERVER_KERBEROS");
      loginContext.login();
      javax.security.auth.login.Configuration jassConfig = 
        javax.security.auth.login.Configuration.getConfiguration();
      AppConfigurationEntry[] appInfo = jassConfig.getAppConfigurationEntry("MAPR_WEBSERVER_KERBEROS");
      for (int i = 0; i < appInfo.length; i++) {
        Map<String, ?> options = appInfo[i].getOptions();
        principal= (String) options.get("principal");
        keytab = (String) options.get("keyTab");
      }


      Subject serverSubject = loginContext.getSubject();
      try {
        gssManager = Subject.doAs(serverSubject, new PrivilegedExceptionAction<GSSManager>() {

          @Override
          public GSSManager run() throws Exception {
            return GSSManager.getInstance();
          }
        });
      } catch (PrivilegedActionException ex) {
        throw ex.getException();
      }
      LOG.info("Initialized, principal ["+ principal + "] from keytab [" + keytab + "]");
    } catch (Exception ex) {
      KerberosUtil.checkJCEKeyStrength();
      LOG.warn("Failed to obtain kerberos identity... " +
          "If no Kerberos configuration was intended no further action is needed" +
          " otherwise turn on DEBUG to see full exception trace");
      if ( LOG.isDebugEnabled()) {
        LOG.debug("Full stacktrace", ex);
      }
      loginContext = null;
      throw new ServletException(ex);
    }
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request, 
      final HttpServletResponse response)
  throws IOException, AuthenticationException {
  AuthenticationToken token = null;
  // at this point authorization should not be null and should be "Negotiate"
  String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);

  if ( authorization != null && authorization.startsWith(KerberosAuthenticator.NEGOTIATE))
  authorization = authorization.substring(KerberosAuthenticator.NEGOTIATE.length()).trim();
  final Base64 base64 = new Base64(0);
  final byte[] clientToken = base64.decode(authorization);
  String negotiateToken = new String(clientToken);
  if (negotiateToken.startsWith("NTLM")) {
    response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, 
        "NTLM Authentication not supported, please try a different browser");
    LOG.info("No support for NTLM tokens is provided");
    return token;
  }

  Subject serverSubject = loginContext.getSubject();
  try {
    token = Subject.doAs(serverSubject, new PrivilegedExceptionAction<AuthenticationToken>() {

      @Override
      public AuthenticationToken run() throws Exception {
        AuthenticationToken token = null;
        GSSContext gssContext = null;
        try {
          gssContext = gssManager.createContext((GSSCredential) null);
          byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length);
          if (serverToken != null && serverToken.length > 0) {
            String authenticate = base64.encodeToString(serverToken);
            response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
                               KerberosAuthenticator.NEGOTIATE + " " + authenticate);
          }
          if (!gssContext.isEstablished()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            LOG.trace("SPNEGO in progress");
          } else {
            String clientPrincipal = gssContext.getSrcName().toString();
            KerberosName kerberosName = new KerberosName(clientPrincipal);
            String userName = kerberosName.getShortName();
            token = new AuthenticationToken(userName, clientPrincipal, getType());
            response.setStatus(HttpServletResponse.SC_OK);
            LOG.trace("SPNEGO completed for principal ["+ clientPrincipal + "]");
          }
        } finally {
          if (gssContext != null) {
            gssContext.dispose();
          }
        }
        return token;
      }
    });
  } catch (PrivilegedActionException ex) {
    if (ex.getException() instanceof IOException) {
      throw (IOException) ex.getException();
    }
    else {
      throw new AuthenticationException(ex.getException());
    }
  } catch (Exception e) {
    throw new AuthenticationException("Authorization is failed, please check your config files settings", e);
  }
  return token;
}

/*  @Override
  public String getType() {
    return "kerberos";
  }
*/
  @Override
  public void destroy() {
    try {
      if (loginContext != null) {
        loginContext.logout();
        loginContext = null;
      }
    } catch (LoginException ex) {
      LOG.warn(ex.getMessage(), ex);
    }
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if ( authorization != null && authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
      return this;
    }
    return null;
  }

  /**
   * Returns the Kerberos principal used by the authentication handler.
   *
   * @return the Kerberos principal used by the authentication handler.
   */
  protected String getPrincipal() {
    return principal;
  }

  /**
   * Returns the keytab used by the authentication handler.
   *
   * @return the keytab used by the authentication handler.
   */
  protected String getKeytab() {
    return keytab;
  }

}
