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

package org.apache.hadoop.security;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.security.rpcauth.RpcAuthMethod;
import org.apache.hadoop.security.rpcauth.RpcAuthRegistry;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class for dealing with SASL on RPC server
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcServer {
  public static final Log LOG = LogFactory.getLog(SaslRpcServer.class);
  public static final String SASL_DEFAULT_REALM = "default";
  public static final String SASL_AUTH_SECRET_MANAGER = "org.apache.hadoop.auth.secret.manager";
  public static final String SASL_KERBEROS_PRINCIPAL = "org.apache.hadoop.auth.kerberos.principal";
  public static final String SASL_AUTH_TOKEN = "org.apache.hadoop.auth.token";

  public static enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");
    
    public final String saslQop;
    
    private QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }
    
    public String getSaslQop() {
      return saslQop;
    }
  }

  public RpcAuthMethod authMethod;
  public String mechanism;
  public String protocol;
  public String serverId;

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public SaslRpcServer(RpcAuthMethod authMethod) throws IOException {
    this.authMethod = authMethod;
    mechanism = authMethod.getMechanismName();
    if (authMethod.equals(RpcAuthRegistry.SIMPLE)) {
      return; // no sasl for simple
    }
    protocol = authMethod.getProtocol();
    serverId = authMethod.getServerId();
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public SaslServer create(final Connection connection,
                           Map<String,Object> saslProperties,
                           SecretManager<TokenIdentifier> secretManager
      ) throws IOException, InterruptedException {
    if (secretManager != null) {
      saslProperties.put(SaslRpcServer.SASL_AUTH_SECRET_MANAGER, secretManager);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("creating SaslServer for authMethod: " + authMethod);
    }
    return authMethod.createSaslServer(connection, saslProperties);
  }

  public static void init(Configuration conf) {
    Security.addProvider(new SaslPlainServer.SecurityProvider());
  }

  static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
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

  /** Authentication method */
  //TODO : Deprecate this after moving all the tests to use UGI.AuthenticationMethod
  @InterfaceStability.Evolving
  public static enum AuthMethod {
    SIMPLE((byte) 80, ""),
    KERBEROS((byte) 81, "GSSAPI"),
    @Deprecated
    DIGEST((byte) 82, "DIGEST-MD5"),
    TOKEN((byte) 82, "DIGEST-MD5"),
    PLAIN((byte) 83, "PLAIN");

    /** The code for this method. */
    public final byte code;
    public final String mechanismName;

    private AuthMethod(byte code, String mechanismName) { 
      this.code = code;
      this.mechanismName = mechanismName;
    }

    private static final int FIRST_CODE = values()[0].code;

    /** Return the object represented by the code. */
    private static AuthMethod valueOf(byte code) {
      final int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length ? null : values()[i];
    }

    /** Return the SASL mechanism name */
    public String getMechanismName() {
      return mechanismName;
    }

    /** Read from in */
    public static AuthMethod read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  };

  // TODO: Consider using this inside RpcAuthMethod implementations.
  // Sasl.createSaslServer is 100-200X slower than caching the factories!
  private static class FastSaslServerFactory implements SaslServerFactory {
    private final Map<String,List<SaslServerFactory>> factoryCache =
        new HashMap<String,List<SaslServerFactory>>();

    FastSaslServerFactory(Map<String,?> props) {
      final Enumeration<SaslServerFactory> factories =
          Sasl.getSaslServerFactories();
      while (factories.hasMoreElements()) {
        SaslServerFactory factory = factories.nextElement();
        for (String mech : factory.getMechanismNames(props)) {
          if (!factoryCache.containsKey(mech)) {
            factoryCache.put(mech, new ArrayList<SaslServerFactory>());
          }
          factoryCache.get(mech).add(factory);
        }
      }
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol,
        String serverName, Map<String,?> props, CallbackHandler cbh)
        throws SaslException {
      SaslServer saslServer = null;
      List<SaslServerFactory> factories = factoryCache.get(mechanism);
      if (factories != null) {
        for (SaslServerFactory factory : factories) {
          saslServer = factory.createSaslServer(
              mechanism, protocol, serverName, props, cbh);
          if (saslServer != null) {
            break;
          }
        }
      }
      return saslServer;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return factoryCache.keySet().toArray(new String[0]);
    }
  }
}
