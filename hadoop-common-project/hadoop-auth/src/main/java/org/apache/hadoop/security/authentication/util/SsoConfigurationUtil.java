package org.apache.hadoop.security.authentication.util;

import com.auth0.jwk.InvalidPublicKeyException;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.SigningKeyNotFoundException;
import com.auth0.jwk.UrlJwkProvider;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.interfaces.RSAPublicKey;
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
  private static volatile SsoConfigurationUtil ssoConfigInstance = null;
  private Map<String, RSAPublicKey> keysMap = new HashMap<>();

  //SSO configuration relates to cluster config, service get it from maprcli command
  public static final String CLIENT_ID = "clientid";
  private final String CLIENT_SECRET = "clientsecret";
  private final String PROVIDER = "providername";
  public static final String ISSUER = "issuerendpoint";

  //SSO configuration relates to Hadoop configuration
  private final String JWS_SSO_ALGORITHM = "jws.sso.algorithm";
  public static final String DEFAULT_JWS_SSO_ALGORITHM = "RS256";

  private final String COOKIE_DOMAIN = "jwt.cookie.domain";

  private final String COOKIE_PATH = "jwt.cookie.path";
  public static final String DEFAULT_COOKIE_PATH = "/";

  private final String COOKIE_NAME = "jwt.cookie.name";
  public static final String DEFAULT_COOKIE_NAME = "hadoop-jwt";

  private final String USER_ATTRIBUTE_NAME = "jwt.user.attribute.name";
  public static final String DEFAULT_USER_ATTRIBUTE_NAME = "preferred_username";

  //Hadoop home related variables
  private static final String HADOOP_HOME_PROPERTY = "hadoop.home.dir";
  private static final String YARN_HOME_PROPERTY = "yarn.home.dir";
  private static final String MAPR_ENV_VAR = "MAPR_HOME";
  private static final String MAPR_PROPERTY_HOME = "mapr.home.dir";
  private static final String MAPR_HOME_PATH_DEFAULT = "/opt/mapr";

  public static boolean useDefaultConf = true;
  public static boolean ssoConfEnabled = false;


  private SsoConfigurationUtil() {
  }

  public static SsoConfigurationUtil getInstance() {
    if (ssoConfigInstance == null) {
      synchronized (SsoConfigurationUtil.class) {
        if (ssoConfigInstance == null) {
          readHadoopSsoConf();
          LOG.debug("Initializing SSO configuration.");
          ssoConfigInstance = new SsoConfigurationUtil();
          ssoConfigMap = new HashMap<>();
          ssoConfigInstance.init();
          ssoConfigInstance.initializePublicKeys();
        }
      }
    }
    return ssoConfigInstance;
  }

  private void init() {
    LOG.debug("Getting SSO configuration from maprcli command.");
    JsonArray result = null;
    String[] ssoConfigCommand = new String[]{"cluster", "getssoconf"};
    Map<String, String> jwtMapConf = new HashMap<>();
    if (ssoConfEnabled) {
      try {
        if(!useDefaultConf) {
          Class<?> jwtKlass = Class.forName("org.apache.hadoop.util.JWTConfiguration");
          Method executeJWTConf = jwtKlass.getMethod("getJWTConfiguration");
          Object jwtConf = jwtKlass.getDeclaredConstructor().newInstance();
          jwtMapConf = (Map) executeJWTConf.invoke(jwtConf);
        }
        Class<?> klass = Class.forName("org.apache.hadoop.util.MaprShellCommandExecutor");
        Method execute = klass.getMethod("execute", String[].class, Map.class, boolean.class);
        Object maprShell = klass.getDeclaredConstructor().newInstance();
        result = (JsonArray) execute.invoke(maprShell, ssoConfigCommand, null, false);

      } catch (Exception ex) {
        LOG.debug("Failed to get SSO configuration from maprcli. Please check 'maprcli cluster getssoconf' command.", ex);
        putEmptyMap();
      }
    }
    if (useDefaultConf) {
      defineDefaultSsoConfMap();
    } else if (jwtMapConf != null && !jwtMapConf.isEmpty()) {
      if (jwtMapConf.get(EXPECTED_JWT_AUDIENCES) != null) {
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

  /**
   * Initialize Map with full list of possible public keys that can be used in IdP
   * */
  private void initializePublicKeys() {
    String certUrl = ssoConfigMap.get(ISSUER) + "/protocol/openid-connect/certs";
    try {
      UrlJwkProvider provider = new UrlJwkProvider(new URI(certUrl).toURL());
      List<Jwk> jwks = provider.getAll();
      for (Jwk jwk : jwks) {
        putKeyIntoMap(jwk);
      }
    } catch (SigningKeyNotFoundException | URISyntaxException | MalformedURLException ex) {
      LOG.error("Service can't get public keys from IdP endpoint.", ex);
    }
  }

  private void putKeyIntoMap(Jwk jwk) {
    try {
      RSAPublicKey publicKey = (RSAPublicKey) jwk.getPublicKey();
      keysMap.put(jwk.getId(), publicKey);
    } catch (InvalidPublicKeyException ex) {
      LOG.error("PublicKey can't be converted to RSAPublicKey.", ex);
    }
  }

  public static void readHadoopSsoConf() {
    String hadoopHome = System.getProperty(HADOOP_HOME_PROPERTY);
    if (hadoopHome == null) {
      hadoopHome = System.getProperty(YARN_HOME_PROPERTY);
      if (hadoopHome == null) {
        String maprHome = System.getenv(MAPR_ENV_VAR);
        if (maprHome == null) {
          maprHome = System.getProperty(MAPR_PROPERTY_HOME);
          if (maprHome == null) {
            maprHome = MAPR_HOME_PATH_DEFAULT;
          }
        }
        String hadoopVer = "";
        try (BufferedReader bufferedReader =
                 new BufferedReader(new FileReader(maprHome + "/hadoop/hadoopversion"))) {
          //hadoopversion must always be one line file
          hadoopVer = bufferedReader.readLine().trim();
        } catch (Exception ex) {
          LOG.warn("Can't read hadoopversion file: {}", ex.getMessage());
        }
        hadoopHome = maprHome + "/hadoop/hadoop-" + hadoopVer;
      }
    }
    try (BufferedReader bufferedReader =
             new BufferedReader(new FileReader(hadoopHome + "/etc/hadoop/ssoConf"))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        if (line.trim().startsWith("ssoEnabled=")) {
          ssoConfEnabled = Boolean.parseBoolean(line.trim().split("=")[1]);
        } else if (line.trim().startsWith("useDefaultConf=")) {
          useDefaultConf = Boolean.parseBoolean(line.trim().split("=")[1]);
        }
      }
    } catch (IOException ex) {
      LOG.error("Failed to parse ssoConf file: {}", ex.getMessage());
    }
  }


  private void defineDefaultSsoConfMap() {
    LOG.debug("Initializing default Hadoop SSO configuration");
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
    ssoConfigMap.put(JWS_SSO_ALGORITHM, DEFAULT_JWS_SSO_ALGORITHM);
    ssoConfigMap.put(COOKIE_DOMAIN, domainName);
    ssoConfigMap.put(COOKIE_PATH, DEFAULT_COOKIE_PATH);
    ssoConfigMap.put(COOKIE_NAME, DEFAULT_COOKIE_NAME);
    ssoConfigMap.put(USER_ATTRIBUTE_NAME, DEFAULT_USER_ATTRIBUTE_NAME);

  }

  private void putEmptyMap() {
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

  public String getJwsSsoAlgorithm() {
    return ssoConfigMap.get(JWS_SSO_ALGORITHM);
  }

  public Map<String, RSAPublicKey> getKeysMap() {
    return keysMap;
  }

  public RSAPublicKey getPublicKey(String kid){
    return keysMap.get(kid);
  }

  /**
   * Return true only if process has access to all main SSO configuration - issuer, client id,
   * client secret and provider name.
   */
  public boolean isSsoEnabled() {
    return !(getClientIssuer().isEmpty() || getClientId().isEmpty() ||
        getClientSecret().isEmpty() || getProvider().isEmpty());
  }

}
