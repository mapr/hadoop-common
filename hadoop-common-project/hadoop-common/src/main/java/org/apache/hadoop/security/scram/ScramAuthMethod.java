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
package org.apache.hadoop.security.scram;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.rpcauth.TokenAuthMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public final class ScramAuthMethod extends TokenAuthMethod {
  private static final Logger LOG = LoggerFactory.getLogger(ScramAuthMethod.class);
  private static final Marker FATAL = MarkerFactory.getMarker("FATAL");
  private String defaultMechanismName = "SCRAM-SHA-256";
  private final String scramPasswordConf = "scram.password";
  private String password;
  private String scramConfig = "scram/scram-site.xml";
  public static final TokenAuthMethod INSTANCE = new ScramAuthMethod();
  CredentialCache credentialCache = null;
  Configuration conf;
  private ScramAuthMethod() {
    super((byte) 84, "tokenScram", "SCRAM-SHA-256", AuthenticationMethod.TOKEN);
  }

  private void createCache() {
    conf = new Configuration();
    conf.addResource(scramConfig);
    credentialCache = new CredentialCache();
    try {
      credentialCache.createCache(defaultMechanismName, ScramCredential.class);
      ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
      password = new String(conf.getPassword(scramPasswordConf));
      ScramCredential generatedCred = formatter.generateCredential(password, 4096);
      CredentialCache.Cache<ScramCredential> sha256Cache = credentialCache.cache(defaultMechanismName, ScramCredential.class);
      sha256Cache.put(UserGroupInformation.getLoginUser().getUserName(), generatedCred);
    } catch (NoSuchAlgorithmException ex) {
      LOG.error(FATAL, "Can't find " + defaultMechanismName + " algorithm.");
    } catch (IOException ex) {
      LOG.error(FATAL, "Exception while getting login user", ex);
      ex.printStackTrace();
    }
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
    if(credentialCache == null){
      createCache();
    }
    return Sasl.createSaslClient(new String[] { mechanismName },
        null, null, SaslRpcServer.SASL_DEFAULT_REALM,
        saslProperties, new ScramClientCallbackHandler(password, token));
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
          "Server is not configured to do SCRAM authentication.");
    }
    if(credentialCache == null){
      createCache();
    }
    return Sasl.createSaslServer(mechanismName, null,
        SaslRpcServer.SASL_DEFAULT_REALM, saslProperties,
        new ScramServerCallbackHandler(credentialCache.cache(defaultMechanismName, ScramCredential.class), secretManager, connection));
  }

}