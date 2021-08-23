package org.apache.hadoop.security.scram;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class ScramServerCallbackHandler implements CallbackHandler {

  private final CredentialCache.Cache<ScramCredential> credentialCache;
  private SecretManager<TokenIdentifier> secretManager;
  private Server.Connection connection;

  public ScramServerCallbackHandler(CredentialCache.Cache<ScramCredential> credentialCache, SecretManager<TokenIdentifier> secretManager,
                                    Server.Connection connection) {
    this.credentialCache = credentialCache;
    this.secretManager = secretManager;
    this.connection = connection;
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
                                                            SecretManager<T> secretManager) throws SecretManager.InvalidToken {
    byte[] tokenId = decodeIdentifier(id);
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
    } catch (IOException e) {
      throw (SecretManager.InvalidToken) new SecretManager.InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }
  public static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    String username = null;
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback)
        username = ((NameCallback) callback).getDefaultName();
      else if (callback instanceof ScramCredentialCallback) {
        TokenIdentifier tokenIdentifier = getIdentifier(username, secretManager);
        connection.attemptingUser = tokenIdentifier.getUser();
        ((ScramCredentialCallback) callback).scramCredential(
            credentialCache.get(UserGroupInformation.getLoginUser().getUserName()));
      } else
        throw new UnsupportedCallbackException(callback);
    }
  }


}
