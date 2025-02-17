package org.apache.hadoop.security.authentication.util;

import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class JWTUtils {

  private static Logger LOG = LoggerFactory.getLogger(JWTUtils.class);

  /**
   * This method provides a single method for validating the JWT for use in
   * request processing. It provides for the override of specific aspects of
   * this implementation through submethods used within but also allows for the
   * override of the entire token validation algorithm.
   *
   * @param jwtToken the token to validate
   * @return true if valid
   */
  public static boolean validateToken(DecodedJWT jwtToken) throws InvalidParameterException {
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
   * Validate that the expiration time of the JWT token has not been violated.
   * If it has then throw an AuthenticationException. Override this method in
   * subclasses in order to customize the expiration validation behavior.
   *
   * @param jwtToken the token that contains the expiration date to validate
   * @return valid true if the token has not expired; false otherwise
   */
  private static boolean validateExpiration(DecodedJWT jwtToken) {
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

  /**
   * Validate whether any of the accepted audience claims is present in the
   * issued token claims list for audience. Override this method in subclasses
   * in order to customize the audience validation behavior.
   *
   * @param jwtToken the JWT token where the allowed audiences will be found
   * @return true if an expected audience is present, otherwise false
   */
  private static boolean validateAudiences(DecodedJWT jwtToken) {
    boolean valid = false;
    List<String> tokenAudienceList = jwtToken.getClaim("aud").asList(String.class);
    // if there were no expected audiences configured then just
    // consider any audience acceptable
    if (SsoConfigurationUtil.getInstance().getAudiences().isEmpty()) {
      valid = true;
    } else {
      // if any of the configured audiences is found then consider it
      // acceptable
      for (String aud : tokenAudienceList) {
        if (SsoConfigurationUtil.getInstance().getAudiences().contains(aud)) {
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
   * Encapsulate the acquisition of the JWT token from HTTP cookies within the
   * request.
   *
   * @param req servlet request to get the JWT token from
   * @return serialized JWT token
   */
  public static String getJWTFromCookie(HttpServletRequest req) {
    String serializedJWT = null;
    Cookie[] cookies = req.getCookies();
    String cookieName = SsoConfigurationUtil.getInstance().getCookieName();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          LOG.info("{} cookie has been found and is being processed", cookieName);
          serializedJWT = cookie.getValue();
          break;
        }
      }
    }
    return serializedJWT;
  }

  /**
   * Replace IP to hostname in URL
   * @param originalUrl original URL
   * @return URL with replaced hostname if there is IP instead of hostname
   * */
  public static String constructURLWithHostname(String originalUrl) {
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
   * Replace hostname in URL
   * @param originalUri old hostname
   * @param newAuthority new hostname
   * @return URL with replaced hostname
   * */
  public static String replaceHostInUrl(URI originalUri, String newAuthority) {
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
}
