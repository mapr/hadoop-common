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

  private static final String COOKIE_DOMAIN = "jwt.cookie.domain";
  private static final String COOKIE_PATH = "jwt.cookie.path";
  private static final String COOKIE_NAME = "jwt.cookie.name";
  private static final String USER_ATTRIBUTE_NAME = "jwt.user.attribute.name";

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

    String cookieDomain = conf.get(COOKIE_DOMAIN, domainName);
    String cookiePath = conf.get(COOKIE_PATH, "/");
    String cookieName = conf.get(COOKIE_NAME, "hadoop-jwt");
    String userAttrName = conf.get(USER_ATTRIBUTE_NAME, "preferred_username");

    jwtConfigMap.put(COOKIE_DOMAIN, cookieDomain);
    jwtConfigMap.put(COOKIE_PATH, cookiePath);
    jwtConfigMap.put(COOKIE_NAME, cookieName);
    jwtConfigMap.put(USER_ATTRIBUTE_NAME, userAttrName);

    return jwtConfigMap;
  }
}
