package org.apache.hadoop.security.authentication.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Tool to getting all configuration for SSO configuration using JWT token from keycloak
 */
public class SsoConfigurationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SsoConfigurationUtil.class);
  public static final String HADOOP_JWT_ENABLED = "hadoop.http.authentication.jwt.enabled";
  public static final String EXPECTED_JWT_AUDIENCES = "hadoop.http.authentication.expected.jwt.audiences";
  private static List<String> audiences = new ArrayList<String>();
  private static Map<String, String> ssoConfigMap = null;
  private static SsoConfigurationUtil ssoConfigInstance = null;
  public static final String CLIENT_ID = "clientid";
  private final String CLIENT_SECRET = "clientsecret";
  private final String PROVIDER = "providername";
  public static final String ISSUER = "issuerendpoint";
  private final String JWS_SSO_ALGORITHM = "jws.sso.algorithm";
  private final String COOKIE_DOMAIN = "jwt.cookie.domain";
  private final String COOKIE_PATH = "jwt.cookie.path";
  private final String COOKIE_NAME = "jwt.cookie.name";
  private final String USER_ATTRIBUTE_NAME = "jwt.user.attribute.name";


  private SsoConfigurationUtil() {
  }

  public static SsoConfigurationUtil getInstance() {
    if (ssoConfigInstance == null) {
      LOG.debug("Initializing SSO configuration.");
      ssoConfigInstance = new SsoConfigurationUtil();
      ssoConfigMap = new HashMap<>();
      ssoConfigInstance.init();
    }
    return ssoConfigInstance;
  }

  private void init() {
    LOG.debug("Getting SSO configuration from maprcli command.");
    JsonArray result = null;
    String[] ssoConfigCommand = new String[]{"cluster", "getssoconf"};
    Map<String, String> jwtMapConf = new HashMap<>();
    try {
      Class<?> jwtKlass = Class.forName("org.apache.hadoop.util.JWTConfiguration");
      Method executeJWTConf = jwtKlass.getMethod("getJWTConfiguration");
      Object jwtConf = jwtKlass.getDeclaredConstructor().newInstance();
      jwtMapConf = (Map) executeJWTConf.invoke(jwtConf);

      Class<?> klass = Class.forName("org.apache.hadoop.util.MaprShellCommandExecutor");
      Method execute = klass.getMethod("execute", String[].class, Map.class, boolean.class);
      Object maprShell = klass.getDeclaredConstructor().newInstance();
      result = (JsonArray) execute.invoke(maprShell, ssoConfigCommand, null, false);
    } catch (Exception ex) {
      LOG.debug("Failed to get SSO configuration from maprcli. Please check 'maprcli cluster getssoconf' command.", ex);
      putEmptyMap();
    }
    if (jwtMapConf != null && !jwtMapConf.isEmpty()) {
      if(jwtMapConf.get(EXPECTED_JWT_AUDIENCES) != null){
        // parse into the list
        audiences.addAll(Arrays.asList(jwtMapConf.get(EXPECTED_JWT_AUDIENCES).split(",")));
      }
      ssoConfigMap.put(JWS_SSO_ALGORITHM, jwtMapConf.get(JWS_SSO_ALGORITHM));
      ssoConfigMap.put(COOKIE_DOMAIN, jwtMapConf.get(COOKIE_DOMAIN));
      ssoConfigMap.put(COOKIE_PATH, jwtMapConf.get(COOKIE_PATH));
      ssoConfigMap.put(COOKIE_NAME, jwtMapConf.get(COOKIE_NAME));
      ssoConfigMap.put(USER_ATTRIBUTE_NAME, jwtMapConf.get(USER_ATTRIBUTE_NAME));
    }
    if (result != null && !result.isEmpty()) {
      JsonElement clientIdJson = result.get(0).getAsJsonObject().get(CLIENT_ID);
      JsonElement clientSecretJson = result.get(0).getAsJsonObject().get(CLIENT_SECRET);
      ssoConfigMap.put(CLIENT_ID, clientIdJson != null ? clientIdJson.getAsString() : "");
      ssoConfigMap.put(CLIENT_SECRET, clientSecretJson != null ? clientSecretJson.getAsString() : "");
      ssoConfigMap.put(PROVIDER, result.get(0).getAsJsonObject().get(PROVIDER).getAsString());
      ssoConfigMap.put(ISSUER, result.get(0).getAsJsonObject().get(ISSUER).getAsString());
    } else {
      putEmptyMap();
    }
  }

  private void putEmptyMap(){
    ssoConfigMap.put(CLIENT_ID, "");
    ssoConfigMap.put(CLIENT_SECRET, "");
    ssoConfigMap.put(PROVIDER, "");
    ssoConfigMap.put(ISSUER, "");
  }

  public Map<String, String> getFullSsoConfig() {
    return ssoConfigMap;
  }

  public String getConf(String key) {
    return ssoConfigMap.get(key);
  }

  public String getClientId() {
    return ssoConfigMap.get(CLIENT_ID);
  }

  public String getClientSecret() {
    return ssoConfigMap.get(CLIENT_SECRET);
  }

  public String getClientIssuer() {
    return ssoConfigMap.get(ISSUER);
  }

  public String getProvider() {
    return ssoConfigMap.get(PROVIDER);
  }

  public String getCookieDomain() {
    return ssoConfigMap.get(COOKIE_DOMAIN);
  }

  public String getCookiePath() {
    return ssoConfigMap.get(COOKIE_PATH);
  }

  public String getCookieName() {
    return ssoConfigMap.get(COOKIE_NAME);
  }

  public String getUserAttrName() {
    return ssoConfigMap.get(USER_ATTRIBUTE_NAME);
  }

  public List<String> getAudiences() {
    return audiences;
  }

  public String getJwsSsoAlgorithm(){
    return ssoConfigMap.get(JWS_SSO_ALGORITHM);
  }

  /**
   * Return true only if process has access to all main SSO configuration - issuer, client id,
   * client secret and provider name.
   * */
  public boolean isSsoEnabled() {
    return !(getClientIssuer().isEmpty() || getClientId().isEmpty() ||
        getClientSecret().isEmpty() || getProvider().isEmpty());
  }

}
