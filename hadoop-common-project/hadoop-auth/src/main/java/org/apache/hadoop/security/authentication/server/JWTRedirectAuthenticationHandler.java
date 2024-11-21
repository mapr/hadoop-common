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

import java.io.*;

import javax.servlet.http.Cookie;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.net.InetAddress;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;


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
  public static final String EXPECTED_JWT_AUDIENCES = "expected.jwt.audiences";
  public static final String JWT_CLIENT_ID = "jwt.client.id";
  public static final String JWT_CLIENT_SECRET = "jwt.client.secret";
  private static final String REDIRECT_URI_QUERY_PARAM = "redirect_uri=";
  private String authenticationProviderUrl = null;
  private List<String> audiences = null;
  private String cookieName = null;
  private String clientId = null;
  private String clientSecret = null;
  private final String delimiter = "&";
  private String cookieDomain;
  private String cookiePath;

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
    clientSecret = config.getProperty(JWT_CLIENT_SECRET, SsoConfigurationUtil.getInstance().getClientSecret());

    cookieDomain = SsoConfigurationUtil.getInstance().getCookieDomain();
    cookiePath = SsoConfigurationUtil.getInstance().getCookiePath();
    cookieName = SsoConfigurationUtil.getInstance().getCookieName();

    // setup the list of valid audiences for token validation
    String auds = config.getProperty(EXPECTED_JWT_AUDIENCES);
    if (auds != null) {
      // parse into the list
      String[] audArray = auds.split(",");
      audiences = new ArrayList<String>();
      for (String a : audArray) {
        audiences.add(a);
      }
    }
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request,
                                              HttpServletResponse response) throws IOException,
      AuthenticationException {
    AuthenticationToken token = null;
    String serializedJWT = null;
    HttpServletRequest req = request;
    serializedJWT = getJWTFromCookie(req);
    if (serializedJWT == null && request.getParameter(CODE) == null) {
      String loginURL = constructLoginURL(request);
      LOG.debug("Sending redirect to: " + loginURL);
      response.sendRedirect(loginURL);
    } else if (serializedJWT == null && request.getParameter(CODE) != null) {
      String jwt = getJWTTokenFromCode(request.getParameter(CODE), request);
      response.addCookie(initCookies(jwt));
      response.sendRedirect(constructURLWithHostname(request.getRequestURL().toString()));
    } else if (serializedJWT != null) {
      String userName = null;
      DecodedJWT jwtToken = JWT.decode(serializedJWT);
      boolean valid = validateToken(jwtToken);
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

  private Cookie initCookies(String jwt) {
    Cookie cookie = null;
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(jwt);
      cookie = createCookie(cookieName, node.get("access_token").asText(), node.get("expires_in").asInt());
    } catch (Exception ex) {
      LOG.error("Can't parse JWT JSON response.");
      LOG.debug("JWT: {}", jwt);
      return null;
    }
    cookie.setPath(cookiePath);
    cookie.setDomain(cookieDomain);
    return cookie;
  }

  private static Cookie createCookie(String name, String val, int exp) {
    Cookie cookie = new Cookie(name, val);
    cookie.setHttpOnly(true);
    cookie.setSecure(true);
    cookie.setMaxAge(exp);
    return cookie;
  }

  public String getJWTTokenFromCode(String code, HttpServletRequest request) throws IOException {
    String urlParameters = "grant_type=authorization_code" + delimiter + "client_id=" + clientId + delimiter +
        "code=" + code + "&client_secret=" + clientSecret + delimiter +
        REDIRECT_URI_QUERY_PARAM + constructURLWithHostname(request.getRequestURL().toString());

    byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);
    URL myurl = URI.create(getTokenUrl()).toURL();
    HttpURLConnection con = (HttpURLConnection) myurl.openConnection();
    StringBuilder content;
    try {
      con.setDoOutput(true);
      con.setRequestMethod("POST");
      con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

      try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
        wr.write(postData);
      }

      try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
        String line;
        content = new StringBuilder();
        while ((line = br.readLine()) != null) {
          content.append(line);
          content.append(System.lineSeparator());
        }
      }
    } finally {
      con.disconnect();
    }
    return content.toString();
  }

  /**
   * Encapsulate the acquisition of the JWT token from HTTP cookies within the
   * request.
   *
   * @param req servlet request to get the JWT token from
   * @return serialized JWT token
   */
  protected String getJWTFromCookie(HttpServletRequest req) {
    String serializedJWT = null;
    Cookie[] cookies = req.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          LOG.info(cookieName
              + " cookie has been found and is being processed");
          serializedJWT = cookie.getValue();
          break;
        }
      }
    }
    return serializedJWT;
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
        REDIRECT_URI_QUERY_PARAM + constructURLWithHostname(request.getRequestURL().toString());
  }


  /**
   * Replace hostname in URL
   * @param originalUri old hostname
   * @param newAuthority new hostname
   * @return URL with replaced hostname
   * */
  private String replaceHostInUrl(URI originalUri, String newAuthority) {
    URI uri;
    try {
      uri = new URI(originalUri.getScheme().toLowerCase(Locale.US), newAuthority,
          originalUri.getPath(), originalUri.getQuery(), originalUri.getFragment());
    } catch (URISyntaxException ex) {
      LOG.warn("Can't create new URI with hostname for host {}", newAuthority);
      ex.printStackTrace();
      return originalUri.toString();
    }
    return uri.toString();
  }

  private String constructURLWithHostname(String originalUrl) {
    try {
      URI originalUri = new URI(originalUrl);
      InetAddress address = InetAddress.getByName(new URL(originalUrl).getHost());
      if (originalUrl.contains(address.getHostAddress())) {
        return replaceHostInUrl(originalUri, address.getHostName() + ":" + originalUri.getPort());
      }
    } catch (Exception ex) {
      LOG.warn("Can't create new URL from request hostname {}. Use URL from request.",
          originalUrl);
    }
    return originalUrl;
  }

  /**
   * This method provides a single method for validating the JWT for use in
   * request processing. It provides for the override of specific aspects of
   * this implementation through submethods used within but also allows for the
   * override of the entire token validation algorithm.
   *
   * @param jwtToken the token to validate
   * @return true if valid
   */
  protected boolean validateToken(DecodedJWT jwtToken) throws InvalidParameterException {
    try {
      DecodedJWT verifiedToken = verifyToken(jwtToken);
      if (verifiedToken == null) {
        LOG.warn("Token validation failed.");
      }
      boolean audValid = validateAudiences(jwtToken);
      if (!audValid) {
        LOG.warn("Audience validation failed.");
      }
      boolean expValid = validateExpiration(jwtToken);
      if (!expValid) {
        LOG.info("Expiration validation failed.");
      }
      return verifiedToken != null && audValid && expValid;

    } catch (Exception e) {
      LOG.error("Exception while validating/introspecting jwt token, check debug logs for more details");
      if (LOG.isDebugEnabled()) {
        e.printStackTrace();
      }
    }
    return false;
  }

  public static DecodedJWT verifyToken(DecodedJWT jwt) throws InvalidParameterException {
    try {
      RSAPublicKey publicKey = loadPublicKey(jwt);
      Algorithm algorithm = Algorithm.RSA256(publicKey, null);
      JWTVerifier verifier = JWT.require(algorithm)
          .withIssuer(jwt.getIssuer())
          .build();

      return verifier.verify(jwt);
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        e.printStackTrace();
      }
      LOG.error("Unable to authenticate: {}", e.getMessage());
      throw new InvalidParameterException("Unable to authenticate: " + e.getMessage());
    }
  }

  private static RSAPublicKey loadPublicKey(DecodedJWT token) throws JwkException, MalformedURLException {
    final String url = getKeycloakCertificateUrl(token);
    JwkProvider provider = new UrlJwkProvider(new URL(url));
    return (RSAPublicKey) provider.get(token.getKeyId()).getPublicKey();
  }

  private static String getKeycloakCertificateUrl(DecodedJWT token) {
    return token.getIssuer() + "/protocol/openid-connect/certs";
  }

  /**
   * Validate whether any of the accepted audience claims is present in the
   * issued token claims list for audience. Override this method in subclasses
   * in order to customize the audience validation behavior.
   *
   * @param jwtToken the JWT token where the allowed audiences will be found
   * @return true if an expected audience is present, otherwise false
   */
  protected boolean validateAudiences(DecodedJWT jwtToken) {
    boolean valid = false;
    List<String> tokenAudienceList = jwtToken.getClaim("aud").asList(String.class);
    // if there were no expected audiences configured then just
    // consider any audience acceptable
    if (audiences == null) {
      valid = true;
    } else {
      // if any of the configured audiences is found then consider it
      // acceptable
      boolean found = false;
      for (String aud : tokenAudienceList) {
        if (audiences.contains(aud)) {
          LOG.debug("JWT token audience has been successfully validated");
          valid = true;
          break;
        }
      }
      if (!valid) {
        LOG.warn("JWT audience validation failed.");
      }
    }
    return valid;
  }

  /**
   * Validate that the expiration time of the JWT token has not been violated.
   * If it has then throw an AuthenticationException. Override this method in
   * subclasses in order to customize the expiration validation behavior.
   *
   * @param jwtToken the token that contains the expiration date to validate
   * @return valid true if the token has not expired; false otherwise
   */
  protected boolean validateExpiration(DecodedJWT jwtToken) {
    boolean valid = false;
    Date expires = jwtToken.getClaim("exp").asDate();
    if (expires == null || new Date().before(expires)) {
      LOG.debug("JWT token expiration date has been "
          + "successfully validated");
      valid = true;
    } else {
      LOG.warn("JWT expiration date validation failed.");
    }
    return valid;
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Bearer realm=\"master\"");
  }
}
