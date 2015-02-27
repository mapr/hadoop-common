/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import java.util.Properties;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.CookieSignatureSecretFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapR implementation of SignerSecretProvider abstract class.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class MapRSignerSecretProvider extends SignerSecretProvider {

  private static Logger LOG = LoggerFactory.getLogger(MapRSignerSecretProvider.class);

  private byte[] secret;
  private byte[][] secrets;

  public MapRSignerSecretProvider() {}

  @Override
  public void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception {
    String signatureSecret = config.getProperty(
      AuthenticationFilter.SIGNATURE_SECRET, "com.mapr.security.maprauth.MaprSignatureSecretFactory");

    Class<?> signatureFactoryClass = Thread.currentThread()
      .getContextClassLoader().loadClass(signatureSecret);

    Object signatureFactory = signatureFactoryClass.newInstance();
    if (signatureFactory instanceof CookieSignatureSecretFactory) {
      signatureSecret = ((CookieSignatureSecretFactory) signatureFactory)
        .getSignatureSecret();
    } else {
      throw new ServletException("The cookie signature secret factory class should implement "
        + CookieSignatureSecretFactory.class.getName() + " interface");
    }

    secret = signatureSecret.getBytes();
    secrets = new byte[][]{secret};
  }

  @Override
  public byte[] getCurrentSecret() {
    return secret;
  }

  @Override
  public byte[][] getAllSecrets() {
    return secrets;
  }
}
