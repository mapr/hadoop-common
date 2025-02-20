package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class JWTConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(JWTConfiguration.class);

  private static final String JWS_SSO_ALGORITHM = "jws.sso.algorithm";
  private static final String DEFAULT_JWS_SSO_ALGORITHM = "RS256";
  private static final String COOKIE_DOMAIN = "jwt.cookie.domain";
  private static final String COOKIE_PATH = "jwt.cookie.path";
  private static final String DEFAULT_COOKIE_PATH = "/";
  private static final String COOKIE_NAME = "jwt.cookie.name";
  private static final String DEFAULT_COOKIE_NAME = "hadoop-jwt";
  private static final String USER_ATTRIBUTE_NAME = "jwt.user.attribute.name";
  private static final String DEFAULT_USER_ATTRIBUTE_NAME = "preferred_username";
  private static final String EXPECTED_JWT_AUDIENCES = "hadoop.http.authentication.expected.jwt.audiences";

  public static Map<String, String> getJWTConfiguration() {
    LOG.debug("Getting JWT configuration from Configuration or init default.");
    Configuration conf = new Configuration();
    Map<String, String> jwtConfigMap = new HashMap<>();

    String domainName = null;
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      String fqdn = localHost.getCanonicalHostName();
      if (fqdn != null && fqdn.contains(".")) {
        domainName = fqdn.substring(fqdn.indexOf(".") + 1);
      }
    } catch (UnknownHostException e) {
      LOG.warn("Can't initialize hostname for the service");
    }

    // setup the list of valid audiences for token validation
    String auds = conf.get(EXPECTED_JWT_AUDIENCES, null);
    if (auds != null) {
      jwtConfigMap.put(EXPECTED_JWT_AUDIENCES, auds);
    }

    jwtConfigMap.put(JWS_SSO_ALGORITHM, conf.get(JWS_SSO_ALGORITHM, DEFAULT_JWS_SSO_ALGORITHM));
    jwtConfigMap.put(COOKIE_DOMAIN, conf.get(COOKIE_DOMAIN, domainName));
    jwtConfigMap.put(COOKIE_PATH, conf.get(COOKIE_PATH, DEFAULT_COOKIE_PATH));
    jwtConfigMap.put(COOKIE_NAME, conf.get(COOKIE_NAME, DEFAULT_COOKIE_NAME));
    jwtConfigMap.put(USER_ATTRIBUTE_NAME, conf.get(USER_ATTRIBUTE_NAME, DEFAULT_USER_ATTRIBUTE_NAME));

    return jwtConfigMap;
  }
}
