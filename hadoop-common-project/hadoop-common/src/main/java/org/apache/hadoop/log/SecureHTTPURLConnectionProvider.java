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
package org.apache.hadoop.log;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.log4j.Logger;

/**
 * 1. Returns a usable https connection if an authenticator is configured;
 * otherwise returns the given connection as is.
 *
 * 2. Caches a authentication token and re-uses it for connections long as it is
 * valid.
 *
 * 3. Performs authentication via configured authenticator only if the token
 * becomes invalid.
 * <p>
 * TODO
 * 1. This code is not cluster aware (it always watches if security is enabled
 * or not on the current cluster)
 *
 * 2. This code is not user aware (the authenticator will pick up creds of the
 * process uid that is running this code)
 */
public class SecureHTTPURLConnectionProvider {
  private static final Logger LOG = Logger.getLogger(SecureHTTPURLConnectionProvider.class);

  private static Token token = new Token();

  private static Authenticator authenticator = null;

  static {
    Configuration conf = new Configuration();
    Class<? extends Authenticator> clazz = conf.getClass(
        CommonConfigurationKeysPublic.LOG_LEVEL_AUTHENTICATOR_CLASS,
        null, Authenticator.class);

    if (clazz != null) {
      try {
        authenticator = clazz.getDeclaredConstructor()
          .newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static URLConnection openConnection(URL url) throws IOException {
    if (authenticator == null) {
      return url.openConnection();
    }

    synchronized (SecureHTTPURLConnectionProvider.class) {

      HttpURLConnection conn = null;
      if (token.isSet()) { // try the existing token
        conn = (HttpURLConnection) url.openConnection();
        AuthenticatedURL.injectToken(conn, token);
        conn.connect();
        if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
          token = new Token(); // refresh the token on auth failure
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received HTTP " + conn.getResponseCode() + ". Created a new token.");
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received HTTP " + conn.getResponseCode() + ". Token still good..");
          }
        }
      }

      if (!token.isSet()) { // authenticate only if the token is not set
        LOG.debug("Token not set. Peforming authentication..");
        AuthenticatedURL authenticatedURL = new AuthenticatedURL(authenticator);
        try {
          conn = authenticatedURL.openConnection(url, token);
        } catch (AuthenticationException e) {
          LOG.error("Authentication failed while connecting to URL: " + url.toString());
        }
      }

      return conn;
    }
  }
}
