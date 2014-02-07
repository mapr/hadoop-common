package org.apache.hadoop.security.rpcauth;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto.Builder;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;

public abstract class RpcAuthMethod {
  private static final String[] LOGIN_MODULES = new String[0];

  @Deprecated
  protected final byte authcode;
  protected final String simpleName;
  protected final String mechanismName;
  protected final AuthenticationMethod authenticationMethod;

  protected RpcAuthMethod(byte code, String simpleName,
      String mechanismName, AuthenticationMethod authMethod) {
    this.authcode = code;
    this.simpleName = simpleName;
    this.mechanismName = mechanismName;
    this.authenticationMethod = authMethod;
  }

  @Deprecated
  public byte getAuthCode() {
    return authcode;
  }

  /** Return the SASL mechanism name */
  public String getMechanismName() {
    return mechanismName;
  }

  public AuthenticationMethod getAuthenticationMethod() {
    return authenticationMethod;
  }

  @Override
  public final int hashCode() {
    return getClass().getName().hashCode();
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that instanceof RpcAuthMethod) {
      RpcAuthMethod other = (RpcAuthMethod)that;
      getClass().getName().equals(other.getClass().getName());
    }
    return false;
  }

  public String[] loginModules() {
    return RpcAuthMethod.LOGIN_MODULES;
  }

  /** Write to out. */
  public void write(DataOutput out) throws IOException {
    out.write(authcode);
  }

  public UserGroupInformation getUser(UserGroupInformation ticket) {
    return ticket;
  }

  public void writeUGI(UserGroupInformation ugi, Builder ugiProto) {
    // default, do-nothing implementation
  }

  public UserGroupInformation getAuthorizedUgi(String authorizedId,
      SecretManager secretManager) throws IOException {
    return UserGroupInformation.createRemoteUser(authorizedId);
  }

  public boolean shouldReLogin() throws IOException {
    return false;
  }

  /** does nothing */
  public void reLogin() throws IOException {
  }

  public boolean isProxyAllowed() {
    return true;
  }

  @Override
  public String toString() {
    return simpleName.toUpperCase();
  }

  /** {@code false} by default */
  public boolean isNegotiable() {
    return false;
  }

  /** {@code false} by default */
  public boolean isSasl() {
    return false;
  }

  public String getProtocol() throws IOException {
      throw new AccessControlException("Server does not support SASL " + this.simpleName.toUpperCase());
  }

  public String getServerId() throws IOException {
    throw new AccessControlException("Server does not support SASL " + this.simpleName.toUpperCase());
  }

  /**
   * Implementors which uses SASL authentication must return {@code true}
   * for {@link #isSasl() isSasl()} method and return and instance of
   * {@link javax.security.sasl.SaslClient}.
   * @throws IOException
   */
  public SaslClient createSaslClient(final Map<String, Object> saslProperties)
      throws IOException {
    throw new UnsupportedOperationException(
        this.getClass().getCanonicalName() + " does not support createSaslClient()");
  }

  /**
   * Implementors which uses SASL authentication must return {@code true}
   * for {@link #isSasl() isSasl()} method and return and instance of
   * {@link javax.security.sasl.SaslServer}.
   * @param connection
   * @throws IOException
   * @throws InterruptedException
   */
  public SaslServer createSaslServer(Server.Connection connection,
      final Map<String, Object> saslProperties)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        this.getClass().getCanonicalName() + " does not support createSaslServer()");
  }

}
