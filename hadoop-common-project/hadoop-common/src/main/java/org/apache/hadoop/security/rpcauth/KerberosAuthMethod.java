package org.apache.hadoop.security.rpcauth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto.Builder;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public final class KerberosAuthMethod extends RpcAuthMethod {
  public static final Log LOG = LogFactory.getLog(KerberosAuthMethod.class);

  static final RpcAuthMethod INSTANCE = new KerberosAuthMethod();
  private KerberosAuthMethod() {
    super((byte) 81, "kerberos", "GSSAPI",
      AuthenticationMethod.KERBEROS);
  }

  private static final String[] LOGIN_MODULES = {
    KerberosUtil.getKrb5LoginModuleName(),
    "com.sun.security.auth.module.Krb5LoginModule"
  };

  @Override
  public String[] loginModules() {
    return LOGIN_MODULES;
  }

  @Override
  public UserGroupInformation getUser(final UserGroupInformation ticket) {
    return (ticket.getRealUser() != null) ? ticket.getRealUser() : ticket;
  }

  @Override
  public void writeUGI(UserGroupInformation ugi, Builder ugiProto) {
    // Send effective user for Kerberos auth
    ugiProto.setEffectiveUser(ugi.getUserName());
  }

  @Override
  public boolean isSasl() {
    return true;
  }

  public boolean isNegotiable() {
    return true;
  }

  @Override
  public String getProtocol() throws IOException {
    String fullName = UserGroupInformation.getCurrentUser().getUserName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Kerberos principal name is " + fullName);
    }
    String[] parts = fullName.split("[/@]", 3);
    return parts.length > 1 ? parts[0] : "";
  }

  @Override
  public String getServerId() throws IOException {
    String fullName = UserGroupInformation.getCurrentUser().getUserName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Kerberos principal name is " + fullName);
    }
    String[] parts = fullName.split("[/@]", 3);
    return (parts.length < 2) ? "" : parts[1];
  }

  @Override
  public SaslClient createSaslClient(final Map<String, Object> saslProperties)
      throws IOException {
    String serverPrincipal = (String)
        saslProperties.get(SaslRpcServer.SASL_KERBEROS_PRINCIPAL);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating SASL " + mechanismName
              + " client. Server's Kerberos principal name is "
              + serverPrincipal);
    }
    if (serverPrincipal == null || serverPrincipal.length() == 0) {
      throw new IOException(
          "Failed to specify server's Kerberos principal name");
    }
    String names[] = splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException(
        "Kerberos principal name does NOT have the expected hostname part: "
              + serverPrincipal);
    }
    return Sasl.createSaslClient(new String[] {mechanismName},
      null, names[0], names[1], saslProperties, null);
 }

  @Override
  public SaslServer createSaslServer(final Server.Connection connection,
      final Map<String, Object> saslProperties)
      throws IOException, InterruptedException {
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String fullName = current.getUserName();
    if (LOG.isDebugEnabled())
      LOG.debug("Kerberos principal name is " + fullName);
    final String names[] = splitKerberosName(fullName);
    if (names.length != 3) {
      throw new AccessControlException(
          "Kerberos principal name does NOT have the expected "
              + "hostname part: " + fullName);
    }
    return current.doAs(
        new PrivilegedExceptionAction<SaslServer>() {
          @Override
          public SaslServer run() throws SaslException {
            return Sasl.createSaslServer(mechanismName, names[0], names[1],
                saslProperties, new SaslGssCallbackHandler());
          }
        });
  }

  @Override
  public synchronized boolean shouldReLogin() throws IOException {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser =
      UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    if (loginUser != null &&
        //Make sure user logged in using Kerberos either keytab or TGT
        loginUser.hasKerberosCredentials() &&
        // relogin only in case it is the login user (e.g. JT)
        // or superuser (like oozie).
        (loginUser.equals(currentUser) || loginUser.equals(realUser))
        ) {
        return true;
    }
    return false;
  }

  @Override
  public void reLogin() throws IOException {
    if (UserGroupInformation.isLoginKeytabBased()) {
      UserGroupInformation.getLoginUser().reloginFromKeytab();
    } else {
      UserGroupInformation.getLoginUser().reloginFromTicketCache();
    }
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static String[] splitKerberosName(String fullName) {
    return fullName.split("[/@]");
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  public static class SaslGssCallbackHandler implements CallbackHandler {

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws
        UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (SaslRpcServer.LOG.isDebugEnabled())
            SaslRpcServer.LOG.debug("SASL server GSSAPI callback: setting "
                + "canonicalized client ID: " + authzid);
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}