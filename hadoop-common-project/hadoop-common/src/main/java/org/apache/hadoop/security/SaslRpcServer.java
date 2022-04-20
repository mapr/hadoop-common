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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Security;
import java.util.Map;

import javax.security.sasl.SaslServer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.security.rpcauth.DigestAuthMethod;
import org.apache.hadoop.security.rpcauth.KerberosAuthMethod;
import org.apache.hadoop.security.rpcauth.RpcAuthMethod;
import org.apache.hadoop.security.rpcauth.RpcAuthRegistry;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for dealing with SASL on RPC server
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcServer {
  public static final Logger LOG = LoggerFactory.getLogger(SaslRpcServer.class);
  public static final String SASL_DEFAULT_REALM = "default";
  public static final String SASL_AUTH_SECRET_MANAGER = "org.apache.hadoop.auth.secret.manager";
  public static final String SASL_KERBEROS_PRINCIPAL = "org.apache.hadoop.auth.kerberos.principal";
  public static final String SASL_AUTH_TOKEN = "org.apache.hadoop.auth.token";

  public enum QualityOfProtection {
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

  static String encodeIdentifier(byte[] identifier) {
    return DigestAuthMethod.encodeIdentifier(identifier);
  }

  static byte[] decodeIdentifier(String identifier) {
    return DigestAuthMethod.decodeIdentifier(identifier);
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
                                                            SecretManager<T> secretManager) throws InvalidToken {
    return DigestAuthMethod.getIdentifier(id, secretManager);
  }

  static char[] encodePassword(byte[] password) {
    return DigestAuthMethod.encodePassword(password);
  }

  /**
   * Splitting fully qualified Kerberos name into parts.
   * @param fullName fullName.
   * @return splitKerberosName.
   */
  public static String[] splitKerberosName(String fullName) {
    return KerberosAuthMethod.splitKerberosName(fullName);
  }

  /** Authentication method */
  //TODO : Deprecate this after moving all the tests to use UGI.AuthenticationMethod
  @InterfaceStability.Evolving
  public enum AuthMethod {
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

    /**
     * Return the SASL mechanism name.
     * @return mechanismName.
     */
    public String getMechanismName() {
      return mechanismName;
    }

    /**
     * Read from in.
     *
     * @param in DataInput.
     * @throws IOException raised on errors performing I/O.
     * @return AuthMethod.
     */
    public static AuthMethod read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /**
     * Write to out.
     * @param out DataOutput.
     * @throws IOException raised on errors performing I/O.
     */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler
          extends org.apache.hadoop.security.rpcauth.DigestAuthMethod.SaslDigestCallbackHandler {
    public SaslDigestCallbackHandler(
            SecretManager<TokenIdentifier> secretManager,
            Server.Connection connection) {
      super(secretManager, connection);
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  @InterfaceStability.Evolving
  public static class SaslGssCallbackHandler
          extends org.apache.hadoop.security.rpcauth.KerberosAuthMethod.SaslGssCallbackHandler {
  }
}
