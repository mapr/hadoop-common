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
package org.apache.hadoop.security.authentication.server;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJWTRedirectAuthenticationHandlerIdToken {

  @Test
  public void testAuthorizationCodeCapturesIdToken() throws Exception {
    String accessToken = encodedJwt(
        "{\"iss\":\"https://issuer\",\"exp\":4102444800,"
            + "\"preferred_username\":\"alice\"}");
    String idToken = encodedJwt(
        "{\"iss\":\"https://issuer\",\"exp\":4102444700,"
            + "\"aud\":\"client\"}");
    StubRedirectHandler handler = new StubRedirectHandler(
        "{\"access_token\":\"" + accessToken
            + "\",\"id_token\":\"" + idToken + "\"}", true);
    AuthenticationToken token = authenticateCode(handler);

    Assert.assertEquals("alice", token.getUserName());
    Assert.assertEquals(idToken, token.getIdToken());
    Assert.assertTrue(token.isJWTBasedToken());
    Assert.assertEquals(4102444700000L, token.getJWTExpires());
  }

  @Test(expected = AuthenticationException.class)
  public void testAuthorizationCodeRejectsMissingIdToken() throws Exception {
    authenticateCode(new StubRedirectHandler(
        "{\"access_token\":\""
            + encodedJwt("{\"exp\":4102444800}") + "\"}", true));
  }

  @Test(expected = AuthenticationException.class)
  public void testAuthorizationCodeRejectsMalformedIdToken() throws Exception {
    authenticateCode(new StubRedirectHandler(
        "{\"access_token\":\""
            + encodedJwt("{\"exp\":4102444800}")
            + "\",\"id_token\":\"malformed\"}", true));
  }

  @Test
  public void testIdTokenMetadataValidation() {
    long future = 4102444800L;
    Assert.assertFalse(JWTUtils.validateIdToken(JWT.decode(encodedJwt(
        "{\"iss\":\"wrong\",\"aud\":\"client\",\"exp\":"
            + future + "}")), "https://issuer", "client"));
    Assert.assertFalse(JWTUtils.validateIdToken(JWT.decode(encodedJwt(
        "{\"iss\":\"https://issuer\",\"aud\":\"wrong\",\"exp\":"
            + future + "}")), "https://issuer", "client"));
    Assert.assertFalse(JWTUtils.validateIdToken(JWT.decode(encodedJwt(
        "{\"iss\":\"https://issuer\",\"aud\":\"client\"}")),
        "https://issuer", "client"));
    Assert.assertFalse(JWTUtils.validateIdToken(JWT.decode(encodedJwt(
        "{\"iss\":\"https://issuer\",\"aud\":\"client\",\"exp\":1}")),
        "https://issuer", "client"));
  }

  private AuthenticationToken authenticateCode(StubRedirectHandler handler)
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(
        JWTRedirectAuthenticationHandler.AUTHENTICATION_PROVIDER_URL,
        "https://issuer");
    properties.setProperty(JWTRedirectAuthenticationHandler.JWT_CLIENT_ID,
        "client");
    handler.init(properties);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getParameter("code")).thenReturn("code");
    return handler.postauthenticate(request,
        Mockito.mock(HttpServletResponse.class));
  }

  private static String encodedJwt(String payload) {
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    return encoder.encodeToString("{\"alg\":\"none\"}"
        .getBytes(StandardCharsets.UTF_8)) + "."
        + encoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8))
        + ".signature";
  }

  private static final class StubRedirectHandler
      extends JWTRedirectAuthenticationHandler {
    private final String responseBody;
    private final boolean valid;

    private StubRedirectHandler(String responseBody, boolean valid) {
      this.responseBody = responseBody;
      this.valid = valid;
    }

    @Override
    public String getJWTTokenFromCode(String code, HttpServletRequest request) {
      return responseBody;
    }

    @Override
    protected boolean validateAccessToken(DecodedJWT jwt) {
      return valid;
    }

    @Override
    protected boolean validateIdToken(DecodedJWT idToken) {
      return valid;
    }
  }
}
