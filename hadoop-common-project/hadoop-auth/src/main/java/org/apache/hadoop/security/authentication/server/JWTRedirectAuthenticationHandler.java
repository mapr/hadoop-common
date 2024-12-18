/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.server;


import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;


/**
 * The {@link JWTRedirectAuthenticationHandler} extends
 * MultiMechsAuthenticationHandler to add WebSSO behavior for UIs. The expected
 * SSO token is a JsonWebToken (JWT). The supported algorithm is RS256 which
 * uses PKI between the token issuer and consumer. The flow requires a redirect
 * to a configured authentication server URL and a subsequent request with the
 * expected JWT token. This token is cryptographically verified and validated.
 * The user identity is then extracted from the token and used to create an
 * AuthenticationToken - as expected by the AuthenticationFilter.
 *
 * <p>
 * The supported configuration properties are:
 * </p>
 * <ul>
 * <li>authentication.provider.url: the full URL to the authentication server.
 * This is the URL that the handler will redirect the browser to in order to
 * authenticate the user. It does not have a default value.</li>
 * <li>expected.jwt.audiences: This is a list of strings that identify
 * acceptable audiences for the JWT token. The audience is a way for the issuer
 * to indicate what entity/s that the token is intended for. Default value is
 * null which indicates that all audiences will be accepted.</li>
 * </ul>
 */
public class JWTRedirectAuthenticationHandler extends
    MultiMechsAuthenticationHandler {
  private static Logger LOG = LoggerFactory
      .getLogger(JWTRedirectAuthenticationHandler.class);

  public static final String AUTHENTICATION_PROVIDER_URL =
      "authentication.provider.url";
  public static final String JWT_CLIENT_ID = "jwt.client.id";
  public static final String JWT_CLIENT_SECRET = "jwt.client.secret";
  private static final String REDIRECT_URI_QUERY_PARAM = "redirect_uri=";
  private String authenticationProviderUrl = null;
  private String clientId = null;
  private final String delimiter = "&";

  private final String CODE = "code";

  /**
   * Initializes the authentication handler instance.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter#init} method.
   * </p>
   *
   * @param config configuration properties to initialize the handler.
   * @throws ServletException thrown if the handler could not be initialized.
   */
  @Override
  public void init(Properties config) throws ServletException {
    // setup the URL to redirect to for authentication
    authenticationProviderUrl = config
        .getProperty(AUTHENTICATION_PROVIDER_URL, SsoConfigurationUtil.getInstance().getClientIssuer());
    if (authenticationProviderUrl == null) {
      throw new ServletException(
          "Authentication provider URL must not be null - configure: "
              + AUTHENTICATION_PROVIDER_URL);
    }
    if (authenticationProviderUrl.endsWith("/")) {
      authenticationProviderUrl = authenticationProviderUrl.substring(0, authenticationProviderUrl.length() - 1);
    }

    clientId = config.getProperty(JWT_CLIENT_ID, SsoConfigurationUtil.getInstance().getClientId());
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request,
                                              HttpServletResponse response) throws IOException,
      AuthenticationException {
    AuthenticationToken token = null;
    String serializedJWT = null;
    HttpServletRequest req = request;
    serializedJWT = JWTUtils.getJWTFromCookie(req);
    if (serializedJWT == null && request.getParameter(CODE) == null) {
      String loginURL = constructLoginURL(request);
      LOG.debug("Sending redirect to: " + loginURL);
      response.sendRedirect(loginURL);
    } else if (serializedJWT == null && request.getParameter(CODE) != null) {
      String jsonJWT = getJWTTokenFromCode(request.getParameter(CODE), request);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(jsonJWT);
      String jwtStr = node.get("access_token").asText();
      DecodedJWT jwt = JWT.decode(jwtStr);
      if (JWTUtils.validateToken(jwt)) {
        String userName = jwt.getClaim(SsoConfigurationUtil.getInstance().getUserAttrName()).asString();
        token = new AuthenticationToken(userName, userName, getType());
        token.setJWTExpires(jwt.getExpiresAt().getTime());
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("Can't add token to cookie, because validating failed.");
        response.sendRedirect(loginURL);
      }
    } else if (serializedJWT != null) {
      String userName = null;
      DecodedJWT jwtToken = JWT.decode(serializedJWT);
      boolean valid = JWTUtils.validateToken(jwtToken);
      if (valid) {
        userName = jwtToken.getClaim(SsoConfigurationUtil.getInstance().getUserAttrName()).asString();
      } else {
        LOG.warn("jwtToken failed validation: " + jwtToken.getToken());
      }
      if (valid) {
        LOG.debug("Issuing AuthenticationToken for user.");
        token = new AuthenticationToken(userName, userName, getType());
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("token validation failed - sending redirect to: " + loginURL);
        response.sendRedirect(loginURL);
      }
    } else {
      LOG.info("JWT can't be found in cookies or get from the authentication server");
    }
    return token;
  }

  public String getJWTTokenFromCode(String code, HttpServletRequest request) throws IOException {
    StringBuilder content;
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(getTokenUrl());
    List<BasicNameValuePair> urlParameters = new ArrayList<>();
    urlParameters.add(new BasicNameValuePair("grant_type", "authorization_code"));
    urlParameters.add(new BasicNameValuePair("client_id", SsoConfigurationUtil.getInstance().getClientId()));
    urlParameters.add(new BasicNameValuePair("code", code));
    urlParameters.add(new BasicNameValuePair("client_secret", SsoConfigurationUtil.getInstance().getClientSecret()));
    urlParameters.add(new BasicNameValuePair("redirect_uri", request.getRequestURL().toString()+"?ssoLogin=processCode"));
    post.setEntity(new UrlEncodedFormEntity(urlParameters));
    HttpResponse response = client.execute(post);

      try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
        String line;
        content = new StringBuilder();
        while ((line = br.readLine()) != null) {
          content.append(line);
          content.append(System.lineSeparator());
        }
      }

    return content.toString();
  }

  public String getTokenUrl() {
    return authenticationProviderUrl + "/protocol/openid-connect/token";
  }

  public String getAuthUrl() {
    return authenticationProviderUrl + "/protocol/openid-connect/auth";
  }


  /**
   * Create the URL to be used for authentication of the user in the absence of
   * a JWT token within the incoming request.
   *
   * @param request for getting the original request URL
   * @return url to use as login url for redirect
   */
  @VisibleForTesting
  String constructLoginURL(HttpServletRequest request) {
    return getAuthUrl() + "?" +
        "response_type=code" + delimiter + "client_id=" + clientId + delimiter + "scope=openid" + delimiter +
        REDIRECT_URI_QUERY_PARAM + JWTUtils.constructURLWithHostname(request.getRequestURL().toString());
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Bearer realm=\"master\"");
  }
}
