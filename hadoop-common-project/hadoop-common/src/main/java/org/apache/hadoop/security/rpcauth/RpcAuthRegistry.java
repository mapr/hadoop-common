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

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

public class RpcAuthRegistry {
  private static final Log LOG =
      LogFactory.getLog(RpcAuthRegistry.class);
  private static final Map<Byte, RpcAuthMethod> authMethods =
      new LinkedHashMap<Byte, RpcAuthMethod>();
  private static final Map<String, RpcAuthMethod> loginModuleMap =
      new LinkedHashMap<String, RpcAuthMethod>();

  /**
   * Pre-defined authentication methods
   */
  public static final RpcAuthMethod KERBEROS = KerberosAuthMethod.INSTANCE;
  public static final RpcAuthMethod FAKE_KERBEROS = FakeKerberosAuthMethod.INSTANCE;
  public static final RpcAuthMethod SIMPLE = SimpleAuthMethod.INSTANCE;
  public static final RpcAuthMethod DIGEST = DigestAuthMethod.INSTANCE;

  static {
    addRpcAuthMethod(SIMPLE);
    addRpcAuthMethod(KERBEROS);
    addRpcAuthMethod(DIGEST);
    addRpcAuthMethod(FAKE_KERBEROS);
  }

  public synchronized static void addRpcAuthMethod(RpcAuthMethod authMethod) {
    if (authMethods.containsKey(authMethod.authcode)) {
      RpcAuthMethod oldMethod = authMethods.get(authMethod.authcode);
      if (!oldMethod.getClass().equals(authMethod.getClass())) {
        throw new IllegalArgumentException(
          String.format("Duplicate authcode [%d] for '%s'. Already registerd for '%s'.",
            authMethod.authcode,
            authMethod.getClass().getCanonicalName(),
            oldMethod.getClass().getCanonicalName()
        ));
      }
    }

    for (String module : authMethod.loginModules()) {
      if (loginModuleMap.containsKey(module)) {
        RpcAuthMethod oldMethod = loginModuleMap.get(module);
        if (!oldMethod.getClass().equals(authMethod.getClass())) {
          throw new IllegalArgumentException(
              String.format("Duplicate login module [%s] for '%s'. Already registerd for '%s'.",
                  module,
                  authMethod.getClass().getCanonicalName(),
                  oldMethod.getClass().getCanonicalName()
                  ));
        }
      }
      loginModuleMap.put(module, authMethod);
    }
    authMethods.put(authMethod.authcode, authMethod);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added " + authMethod + " to registry.");
    }
  }

  /** Return the RpcAuthMethod for given JAAS login module */
  public static RpcAuthMethod getAuthMethodForLoginModule(String loginModule) {
    return loginModuleMap.get(loginModule);
  }

  /** Return the RpcAuthMethod for given auth code */
  public static RpcAuthMethod getAuthMethod(byte authCode) {
    return authMethods.get(authCode);
  }

  public static RpcAuthMethod getAuthMethod(String name) {
    for (RpcAuthMethod method : authMethods.values()) {
      if (method.simpleName.equalsIgnoreCase(name)) {
        return method;
      }
    }
    LOG.warn("No RpcAuthMethod registerd for name " + name);
    return null;
  }

  public static RpcAuthMethod getAuthMethod(AuthenticationMethod authenticationMethod) {
    for (RpcAuthMethod method : authMethods.values()) {
      if (method.authenticationMethod.equals(authenticationMethod)) {
        return method;
      }
    }
    LOG.warn("No RpcAuthMethod registerd for authentication method " + authenticationMethod);
    return null;
  }

  /** Read from in. */
  @Deprecated
  public static RpcAuthMethod readAuthMethod(DataInput in) throws IOException {
    byte code = in.readByte();
    if (!authMethods.containsKey(code)) {
      LOG.warn("No RpcAuthMethod registerd for auth code " + code);
    }
    return authMethods.get(code);
  }

  /**
   * Return the ordered list of auth method for given comma separated names.
   * To be used for logging purpose only.
   */
  @Deprecated
  public static List<RpcAuthMethod> getAuthMethodList(byte[] authCodes) {
    List<RpcAuthMethod> list = new ArrayList<RpcAuthMethod>();
    for (byte code : authCodes) {
      RpcAuthMethod method = authMethods.get(code);
      if (method == null) {
        String name = "UNKNOWN(" + code + ")";
        method = new RpcAuthMethod(code, name, name, null) {};
      }
      list.add(method);
    }
    return list;
  }

}
