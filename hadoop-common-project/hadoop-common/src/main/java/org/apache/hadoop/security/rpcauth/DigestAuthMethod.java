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
package org.apache.hadoop.security.rpcauth;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

//TODO: rename this class to TokenAuthMethod and use the mechanism as "token"
// This is because UGI.AuthenticationMethod doesn't have DIGEST anymore and
// hadoop is deprecating SaslRpcServer.AuthMethod which has DIGEST already
// deprecated.
public final class DigestAuthMethod extends RpcAuthMethod {
  private static final Log LOG = LogFactory.getLog(DigestAuthMethod.class);

  static final RpcAuthMethod INSTANCE = new DigestAuthMethod();
  private DigestAuthMethod() {
    super((byte) 82, "token", "DIGEST-MD5", AuthenticationMethod.TOKEN);
  }

  @Override
  public boolean isProxyAllowed() {
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public UserGroupInformation getAuthorizedUgi(String authorizedId,
      SecretManager secretManager) throws IOException {
    TokenIdentifier tokenId = getIdentifier(authorizedId, secretManager);
    UserGroupInformation ugi = tokenId.getUser();
    if (ugi == null) {
      throw new AccessControlException(
          "Can't retrieve username from tokenIdentifier.");
    }
    ugi.addTokenIdentifier(tokenId);
    return ugi;
  }

  @Override
  public boolean isSasl() {
    return true;
  }

  @Override
  public String getProtocol() throws IOException {
    return SaslRpcServer.SASL_DEFAULT_REALM;
  }

  @Override
  public String getServerId() throws IOException {
    return "";
  }

  @Override
  @SuppressWarnings("unchecked")
  public SaslClient createSaslClient(final Map<String, Object> saslProperties)
      throws IOException {
    Token<? extends TokenIdentifier> token = (Token<? extends TokenIdentifier>)
        saslProperties.get(SaslRpcServer.SASL_AUTH_TOKEN);
    if (LOG.isDebugEnabled())
      LOG.debug("Creating SASL " + mechanismName
          + " client to authenticate to service at " + token.getService());
    return Sasl.createSaslClient(new String[] { mechanismName },
        null, null, SaslRpcServer.SASL_DEFAULT_REALM,
        saslProperties, new SaslClientCallbackHandler(token));
  }

  @Override
  @SuppressWarnings("unchecked")
  public SaslServer createSaslServer(final Server.Connection connection,
      final Map<String, Object> saslProperties)
      throws IOException {
    SecretManager<TokenIdentifier> secretManager = (SecretManager<TokenIdentifier>)
        saslProperties.get(SaslRpcServer.SASL_AUTH_SECRET_MANAGER);
    if (secretManager == null) {
      throw new AccessControlException(
          "Server is not configured to do DIGEST authentication.");
    }
    return Sasl.createSaslServer(mechanismName, null,
        SaslRpcServer.SASL_DEFAULT_REALM, saslProperties,
        new SaslDigestCallbackHandler(secretManager, connection));
  }

  public static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password)).toCharArray();
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
      SecretManager<T> secretManager) throws InvalidToken {
    byte[] tokenId = decodeIdentifier(id);
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  public static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  public static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = encodeIdentifier(token.getIdentifier());
      this.userPassword = encodePassword(token.getPassword());
    }

    @Override
    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting username: " + userName);
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting realm: "
              + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  public static class SaslDigestCallbackHandler implements CallbackHandler {
    private SecretManager<TokenIdentifier> secretManager;
    private Server.Connection connection;

    public SaslDigestCallbackHandler(SecretManager<TokenIdentifier> secretManager,
        Server.Connection connection) {
      this.secretManager = secretManager;
      this.connection = connection;
    }

    private char[] getPassword(TokenIdentifier tokenid) throws InvalidToken {
      return encodePassword(secretManager.retrievePassword(tokenid));
    }

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws InvalidToken,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        TokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(), secretManager);
        char[] password = getPassword(tokenIdentifier);
        UserGroupInformation user = null;
        user = tokenIdentifier.getUser(); // may throw exception
        connection.attemptingUser = user;
        if (SaslRpcServer.LOG.isDebugEnabled()) {
          SaslRpcServer.LOG.debug("SASL server DIGEST-MD5 callback: setting password "
              + "for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
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
          if (SaslRpcServer.LOG.isDebugEnabled()) {
            String username =
              getIdentifier(authzid, secretManager).getUser().getUserName();
            SaslRpcServer.LOG.debug("SASL server DIGEST-MD5 callback: setting "
                + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}