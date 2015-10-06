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
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.MapRModified;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.login.HadoopLoginModule;
import org.apache.hadoop.security.rpcauth.RpcAuthMethod;
import org.apache.hadoop.security.rpcauth.RpcAuthRegistry;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Time;


import com.google.common.annotations.VisibleForTesting;

/**
 * User and group information for Hadoop.
 * This class wraps around a JAAS Subject and provides methods to determine the
 * user's username and groups. It supports both the Windows, Unix and Kerberos 
 * login modules.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase", "Hive", "Oozie"})
@InterfaceStability.Evolving
@MapRModified
public class UserGroupInformation {
  private static final Log LOG =  LogFactory.getLog(UserGroupInformation.class);
  /**
   * Percentage of the ticket window to use before we renew ticket.
   */
  private static final float TICKET_RENEW_WINDOW = 0.80f;
  private static boolean shouldRenewImmediatelyForTests = false;
  static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  static final String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";

  /**
   * For the purposes of unit tests, we want to test login
   * from keytab and don't want to wait until the renew
   * window (controlled by TICKET_RENEW_WINDOW).
   * @param immediate true if we should login without waiting for ticket window
   */
  @VisibleForTesting
  public static void setShouldRenewImmediatelyForTests(boolean immediate) {
    shouldRenewImmediatelyForTests = immediate;
  }

  // Begin MapR section
  private static final String HADOOP_SECURITY_SPOOF_USER = "hadoop.spoof.user";
  private static final String HADOOP_SECURITY_SPOOFED_USER = "hadoop.spoofed.user.username";
  private static boolean spoofUser = false;
  private static String spoofedUser;

  private static void checkSpoofing(Configuration conf) {
    // spoof users by default on Windows
    spoofUser = conf.getBoolean(HADOOP_SECURITY_SPOOF_USER,
        windows ? true : false);

    if (!spoofUser) {
      return;
    }

    spoofedUser = conf.get(HADOOP_SECURITY_SPOOFED_USER, "root");
  }
  // End MapR section

  /** 
   * UgiMetrics maintains UGI activity statistics
   * and publishes them through the metrics interfaces.
   */
  @Metrics(about="User and group related metrics", context="ugi")
  static class UgiMetrics {
    final MetricsRegistry registry = new MetricsRegistry("UgiMetrics");

    @Metric("Rate of successful kerberos logins and latency (milliseconds)")
    MutableRate loginSuccess;
    @Metric("Rate of failed kerberos logins and latency (milliseconds)")
    MutableRate loginFailure;
    @Metric("GetGroups") MutableRate getGroups;
    MutableQuantiles[] getGroupsQuantiles;

    static UgiMetrics create() {
      return DefaultMetricsSystem.instance().register(new UgiMetrics());
    }

    void addGetGroups(long latency) {
      getGroups.add(latency);
      if (getGroupsQuantiles != null) {
        for (MutableQuantiles q : getGroupsQuantiles) {
          q.add(latency);
        }
      }
    }
  }

  /** Metrics to track UGI activity */
  static UgiMetrics metrics = UgiMetrics.create();
  /** The auth method to use */
  private static AuthenticationMethod authenticationMethod;
  /** Server-side groups fetching service */
  private static Groups groups;
  /** The configuration to use */
  private static Configuration conf;

  /** JAAS configuration name to be used for user login */
  private static String userJAASConfName;

  /** JAAS configuration name to be used for service login */
  private static String serviceJAASConfName;
  
  /** Leave 10 minutes between relogin attempts. */
  private static final long MIN_TIME_BEFORE_RELOGIN = 10 * 60 * 1000L;
  
  /**Environment variable pointing to the token cache file*/
  public static final String HADOOP_TOKEN_FILE_LOCATION = 
    "HADOOP_TOKEN_FILE_LOCATION";

  private static Class<? extends Principal> customAuthPrincipalClass;

  private static Class<? extends RpcAuthMethod> customRpcAuthMethodClass;

  // mapr_extensibility
  public static final String USER_TICKET_FILE_LOCATION = "MAPR_TICKETFILE_LOCATION";

  public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

  /** 
   * A method to initialize the fields that depend on a configuration.
   * Must be called before useKerberos or groups is used.
   */
  private static void ensureInitialized() {
    if (conf == null) {
      synchronized(UserGroupInformation.class) {
        if (conf == null) { // someone might have beat us
          initialize(new Configuration(), false);
        }
      }
    }
  }

  /**
   * Initialize UGI and related classes.
   * @param conf the configuration to use
   */
  private static synchronized void initialize(Configuration conf,
                                              boolean overrideNameRules) {
    AuthenticationMethod authenticationMethodFromConf = getAuthenticationMethodFromConfiguration(conf);
    if (LOG.isDebugEnabled())
      LOG.debug("HADOOP_SECURITY_AUTHENTICATION is set to: " + authenticationMethodFromConf);

    // spoofing is only allowed when insecure
    if (authenticationMethodFromConf == null || authenticationMethodFromConf.equals(AuthenticationMethod.SIMPLE)) {
      checkSpoofing(conf);
    } else {
      // Initialize custom auth method.
      // TODO This code really belongs to RpcAuthRegistry.
      customAuthPrincipalClass = SecurityUtil.getCustomAuthPrincipal(conf);
      customRpcAuthMethodClass = SecurityUtil.getCustomRpcAuthMethod(conf);
    }

    String jaasConfName = null;
    if (authenticationMethodFromConf == AuthenticationMethod.SIMPLE) {
      jaasConfName = "simple";
    } else {
      LOG.debug("Security is enabled.");
      // ignore what is passed in and instead honor extra configuration
      // or JVM setting if neither set, use default.
      jaasConfName = System.getProperty("hadoop.login");
      if (jaasConfName == null) {
        jaasConfName = conf.get("hadoop.login", "default");
      }
    }

    userJAASConfName = jaasConfName.startsWith("hadoop_")
        ? jaasConfName : "hadoop_" + jaasConfName;
    serviceJAASConfName = userJAASConfName.endsWith("_keytab")
        ? userJAASConfName : userJAASConfName + "_keytab";

    if (LOG.isDebugEnabled())
      LOG.debug("Login configuration entry is " + userJAASConfName);

    String loginConfPath = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    
    if (loginConfPath == null) {
      loginConfPath = conf.get(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
      if (loginConfPath != null ) {
        System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, loginConfPath);
        loginConfPath = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
        LOG.info("Java System property 'java.security.auth.login.config' not"
            + " set, unilaterally setting to " + loginConfPath);
      }
    }
    // still can be null
    if ( loginConfPath == null || !(new File(loginConfPath)).canRead() ) {
      if ( LOG.isDebugEnabled()) {
        LOG.debug(loginConfPath + " either null or can not be read. Trying to load " 
        + "'java.security.auth.login.config' from jar");
      }
      String javaSecurityJarPath = 
          conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURITY_JAR_PATH);
      URL javaSecurityURL = null;
      if ( javaSecurityJarPath != null ) {
        javaSecurityURL = UserGroupInformation.class.getResource(javaSecurityJarPath); 
        if ( javaSecurityURL != null ) {
          loginConfPath = javaSecurityURL.toExternalForm();
          System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, loginConfPath);
          if ( LOG.isDebugEnabled()) {
            LOG.debug("Loading 'java.security.auth.login.config' from: " + loginConfPath);
          }
        } 
      } 
      if (javaSecurityJarPath == null || javaSecurityURL == null ) {
        LOG.warn("'java.security.auth.login.config' is not"
            + " configured either in Hadoop configuration or"
            + " via Java property or not loaded from jar, may cause login failure");
      }  
    }
    // by this time JAAS config file is fully loaded. Set UGI's authenticationMethod from JAAS config.
    setUGIAuthenticationMethodFromJAASConfiguration(jaasConfName);

    if (overrideNameRules || !HadoopKerberosName.hasRulesBeenSet()) {
      try {
        HadoopKerberosName.setConfiguration(conf);
      } catch (IOException ioe) {
        throw new RuntimeException(
            "Problem with Kerberos auth_to_local name configuration", ioe);
      }
    }
    // If we haven't set up testing groups, use the configuration to find it
    if (!(groups instanceof TestingGroups)) {
      groups = Groups.getUserToGroupsMappingService(conf);
    }
    UserGroupInformation.conf = conf;

    if (metrics.getGroupsQuantiles == null) {
      int[] intervals = conf.getInts(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS);
      if (intervals != null && intervals.length > 0) {
        final int length = intervals.length;
        MutableQuantiles[] getGroupsQuantiles = new MutableQuantiles[length];
        for (int i = 0; i < length; i++) {
          getGroupsQuantiles[i] = metrics.registry.newQuantiles(
            "getGroups" + intervals[i] + "s",
            "Get groups", "ops", "latency", intervals[i]);
        }
        metrics.getGroupsQuantiles = getGroupsQuantiles;
      }
    }
  }

  private static AuthenticationMethod getAuthenticationMethodFromConfiguration(Configuration conf) {
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class, value.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  /**
   * The only supported options in JAAS config file (currently) are SIMPLE, KERBEROS and CUSTOM.
   *
   * DIGEST/TOKEN is internal to hadoop. PROXY is used during impersonation. These two options
   * are therefore not never returned via this method.
   */
  private static void setUGIAuthenticationMethodFromJAASConfiguration(String jaasConfName) {
    if (jaasConfName.contains("simple")) {
      UserGroupInformation.authenticationMethod = AuthenticationMethod.SIMPLE;
    } else if (jaasConfName.contains("kerberos")) {
      UserGroupInformation.authenticationMethod = AuthenticationMethod.KERBEROS;
    } else { // if it's not SIMPLE and not KERBEROS, treat it as CUSTOM
      UserGroupInformation.authenticationMethod = AuthenticationMethod.CUSTOM;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("authenticationMethod from JAAS configuration:" + UserGroupInformation.authenticationMethod);
    }
  }

  /**
   * Returns authenticationMethod obtained by inspecting JAAS configuration.
   * @return
   */
  public static AuthenticationMethod getUGIAuthenticationMethod() {
    if (UserGroupInformation.authenticationMethod == null) {
      ensureInitialized();
    }
    return UserGroupInformation.authenticationMethod;
  }

  /**
   * Set the static configuration for UGI.
   * In particular, set the security authentication mechanism and the
   * group look up service.
   * @param conf the configuration to use
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void setConfiguration(Configuration conf) {
    initialize(conf, true);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public static void reset() {
    authenticationMethod = null;
    conf = null;
    groups = null;
    setLoginUser(null);
    HadoopKerberosName.setRules(null);
  }
  
  /**
   * Determine if UserGroupInformation is using Kerberos to determine
   * user identities or is relying on simple authentication
   * 
   * @return true if UGI is working in a secure environment
   */
  public static boolean isSecurityEnabled() {
    return !isAuthenticationMethodEnabled(AuthenticationMethod.SIMPLE);
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  private static boolean isAuthenticationMethodEnabled(AuthenticationMethod method) {
    ensureInitialized();
    return (authenticationMethod == method);
  }
  
  /**
   * Information about the logged in user.
   */
  private static UserGroupInformation loginUser = null;
  private static String keytabPrincipal = null;
  private static String keytabFile = null;

  private final Subject subject;
  // All non-static fields must be read-only caches that come from the subject.
  private final User user;
  private final boolean isKeytab;
  private final boolean isKrbTkt;
  private List<RpcAuthMethod> rpcAuthMethodList;

  private static final boolean windows = 
      System.getProperty("os.name").startsWith("Windows");

  private static class RealUser implements Principal {
    private final UserGroupInformation realUser;
    
    RealUser(UserGroupInformation realUser) {
      this.realUser = realUser;
    }
    
    @Override
    public String getName() {
      return realUser.getUserName();
    }
    
    public UserGroupInformation getRealUser() {
      return realUser;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      } else {
        return realUser.equals(((RealUser) o).realUser);
      }
    }
    
    @Override
    public int hashCode() {
      return realUser.hashCode();
    }
    
    @Override
    public String toString() {
      return realUser.toString();
    }
  }

  private static LoginContext
  newLoginContext(String appName, Subject subject,
    Map<String,?> overrideOptions)
      throws LoginException {
    // Temporarily switch the thread's ContextClassLoader to match this
    // class's classloader, so that we can properly load HadoopLoginModule
    // from the JAAS libraries.
    Thread t = Thread.currentThread();
    ClassLoader oldCCL = t.getContextClassLoader();
    t.setContextClassLoader(HadoopLoginModule.class.getClassLoader());
    try {
      if (overrideOptions != null) {
        javax.security.auth.login.Configuration cfg =
            new DynamicLoginConfiguration(getJAASConf(), overrideOptions);
        return new LoginContext(appName, subject, null, cfg);
      }
      return new LoginContext(appName, subject);
    } finally {
      t.setContextClassLoader(oldCCL);
    }
  }

  private static LoginContext newLoginContextWithKeyTab(String appName,
      Subject subject, String keytab, String principal) throws LoginException {
    Map<String,String> overrideOptions = new HashMap<String, String>();
    overrideOptions.put("keyTab", keytab);
    overrideOptions.put("principal", principal);
    return newLoginContext(appName, subject, overrideOptions);
  }

  private static javax.security.auth.login.Configuration getJAASConf() {
    return javax.security.auth.login.Configuration.getConfiguration();
  }

  private LoginContext getLogin() {
    return user.getLogin();
  }
  
  private void setLogin(LoginContext login) {
    user.setLogin(login);
  }

  private void configureRpcAuthMethods(String confName) {
    ArrayList<RpcAuthMethod> authMethodList = new ArrayList<RpcAuthMethod>();
    if (isSecurityEnabled() && confName != null) {
      Set<RpcAuthMethod> authMethods = new LinkedHashSet<RpcAuthMethod>();
      AppConfigurationEntry[] appInfo =
          getJAASConf().getAppConfigurationEntry(confName);
      for (int i = 0; appInfo != null && i < appInfo.length; i++) {
        String module = appInfo[i].getLoginModuleName();
        RpcAuthMethod rpcAuthMethod = RpcAuthRegistry.getAuthMethodForLoginModule(module);
        if (rpcAuthMethod != null) {
          authMethods.add(rpcAuthMethod);
        }
      }

      if (isSecurityEnabled()
          && !"hadoop_simple".equals(confName)
          && authMethods.size() == 0) {
        LOG.warn("Security is enabled but no suitable RPC authentication "
            + "method is found in the provided JAAS configuration: " + confName);
      }
      authMethodList.addAll(authMethods);
    } else {
      authMethodList.add(RpcAuthRegistry.SIMPLE);
    }

    this.rpcAuthMethodList = Collections.unmodifiableList(authMethodList);
  }

  /**
   * Create a UserGroupInformation for the given subject.
   * This does not change the subject or acquire new credentials.
   * @param subject the user's subject
   */
  UserGroupInformation(Subject subject) {
    this(subject, userJAASConfName);
  }

  UserGroupInformation(Subject subject, String loginConfName) {
    this.subject = subject;
    this.user = subject.getPrincipals(User.class).iterator().next();
    this.isKeytab = !subject.getPrivateCredentials(KeyTab.class).isEmpty();
    this.isKrbTkt = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
    configureRpcAuthMethods(loginConfName);

    /* Since multiple methods are allowed at once, this isn't a great setting.
     * We just prefer the dominant method. This concept really should be removed.
     * Better to have the login modules set the methods of authentication based
     * upon some common mapping or even callers just look directly at the subject
     * to determine what is in it.
     */
    if (!subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
      this.user.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
      LOG.debug("found Kerberos Principal in subject, marking as such");
    } else if (customAuthPrincipalClass != null && !subject.getPrincipals(customAuthPrincipalClass).isEmpty()) {
      this.user.setAuthenticationMethod(AuthenticationMethod.CUSTOM);
      LOG.debug("found custom auth principal " + customAuthPrincipalClass.getName() + "  in subject. " +
          "marking authentication method as " + AuthenticationMethod.CUSTOM);
    }
  }
  
  /**
   * checks if logged in using kerberos
   * @return true if the subject logged via keytab or has a Kerberos TGT
   */
  public boolean hasKerberosCredentials() {
    return isKeytab || isKrbTkt;
  }

  public List<RpcAuthMethod> getRpcAuthMethodList() {
    return rpcAuthMethodList;
  }

  /**
   * Return the current user, including any doAs in the current stack.
   * @return the current user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized
  static UserGroupInformation getCurrentUser() throws IOException {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject == null || subject.getPrincipals(User.class).isEmpty()) {
      return getLoginUser();
    } else {
      return new UserGroupInformation(subject);
    }
  }

  /**
   * Find the most appropriate UserGroupInformation to use
   *
   * @param ticketCachePath    The Kerberos ticket cache path, or NULL
   *                           if none is specfied
   * @param user               The user name, or NULL if none is specified.
   *
   * @return                   The most appropriate UserGroupInformation
   */ 
  public static UserGroupInformation getBestUGI(
      String ticketCachePath, String user) throws IOException {
    if (ticketCachePath != null) {
      return getUGIFromTicketCache(ticketCachePath, user);
    } else if (user == null) {
      return getCurrentUser();
    } else {
      return createRemoteUser(user);
    }    
  }

  /**
   * Create a UserGroupInformation from a Kerberos ticket cache.
   * 
   * @param user                The principal name to load from the ticket
   *                            cache
   * @param ticketCachePath     the path to the ticket cache file
   *
   * @throws IOException        if the kerberos login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation getUGIFromTicketCache(
            String ticketCache, String user) throws IOException {
    if (!isAuthenticationMethodEnabled(AuthenticationMethod.KERBEROS)) {
      return getBestUGI(null, user);
    }
    try {
      Map<String,String> krbOptions = new HashMap<String,String>();
      if (IBM_JAVA) {
        krbOptions.put("useDefaultCcache", "true");
        // The first value searched when "useDefaultCcache" is used.
        System.setProperty("KRB5CCNAME", ticketCache);
      } else {
        krbOptions.put("ticketCache", ticketCache);
      }
      krbOptions.put("renewTGT", "false");
      LoginContext login = newLoginContext(userJAASConfName, null, krbOptions);
      login.login();

      Subject loginSubject = login.getSubject();
      Set<Principal> loginPrincipals = loginSubject.getPrincipals();
      if (loginPrincipals.isEmpty()) {
        throw new RuntimeException("No login principals found!");
      }
      if (loginPrincipals.size() != 1) {
        LOG.warn("found more than one principal in the ticket cache file " +
          ticketCache);
      }
      User ugiUser = new User(loginPrincipals.iterator().next().getName(),
          AuthenticationMethod.KERBEROS, login);
      loginSubject.getPrincipals().add(ugiUser);
      UserGroupInformation ugi = new UserGroupInformation(loginSubject);
      ugi.setLogin(login);
      ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
      return ugi;
    } catch (LoginException le) {
      throw new IOException("failure to login using ticket cache file " +
          ticketCache, le);
    }
  }

   /**
   * Create a UserGroupInformation from a Subject with Kerberos principal.
   *
   * @param user                The KerberosPrincipal to use in UGI
   *
   * @throws IOException        if the kerberos login fails
   */
  public static UserGroupInformation getUGIFromSubject(Subject subject)
      throws IOException {
    if (subject == null) {
      throw new IOException("Subject must not be null");
    }

    if (subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
      throw new IOException("Provided Subject must contain a KerberosPrincipal");
    }

    KerberosPrincipal principal =
        subject.getPrincipals(KerberosPrincipal.class).iterator().next();

    User ugiUser = new User(principal.getName(),
        AuthenticationMethod.KERBEROS, null);
    subject.getPrincipals().add(ugiUser);
    UserGroupInformation ugi = new UserGroupInformation(subject);
    ugi.setLogin(null);
    ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    return ugi;
  }

  /**
   * Get the currently logged in user.
   * @return the logged in user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized 
  static UserGroupInformation getLoginUser() throws IOException {
    if (loginUser == null) {
      loginUserFromSubject(null);
    }
    return loginUser;
  }

  /**
   * remove the login method that is followed by a space from the username
   * e.g. "jack (auth:SIMPLE)" -> "jack"
   *
   * @param userName
   * @return userName without login method
   */
  public static String trimLoginMethod(String userName) {
    int spaceIndex = userName.indexOf(' ');
    if (spaceIndex >= 0) {
      userName = userName.substring(0, spaceIndex);
    }
    return userName;
  }

  /**
   * Log in a user using the given subject
   * @parma subject the subject to use when logging in a user, or null to 
   * create a new subject.
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized 
  static void loginUserFromSubject(Subject subject) throws IOException {
    ensureInitialized();
    try {
      if (subject == null) {
        subject = new Subject();
      }
      LoginContext login = newLoginContext(userJAASConfName, subject, null);
      login.login();
      UserGroupInformation realUser = new UserGroupInformation(subject, userJAASConfName);
      realUser.setLogin(login);
      // AuthenticationMethod is now computed based upon subject
      // loginUser.setAuthenticationMethod(...);
      setUserAuthenticationMethod(realUser);
      realUser = new UserGroupInformation(login.getSubject(), userJAASConfName);
      // If the HADOOP_PROXY_USER environment variable or property
      // is specified, create a proxy user as the logged in user.
      String proxyUser = System.getenv(HADOOP_PROXY_USER);
      if (proxyUser == null) {
        proxyUser = System.getProperty(HADOOP_PROXY_USER);
      }
      loginUser = proxyUser == null ? realUser : createProxyUser(proxyUser, realUser);

      String fileLocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      if (fileLocation != null) {
        // Load the token storage file and put all of the tokens into the
        // user. Don't use the FileSystem API for reading since it has a lock
        // cycle (HADOOP-9212).
        Credentials cred = Credentials.readTokenStorageFile(
            new File(fileLocation), conf);
        loginUser.addCredentials(cred);
      }
      loginUser.spawnAutoRenewalThreadForUserCreds();
    } catch (LoginException le) {
      LOG.debug("failure to login", le);
      throw new IOException("failure to login: " + le.getMessage(), le);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("UGI loginUser:"+loginUser);
    }
  }

  private static void setUserAuthenticationMethod(UserGroupInformation realUser) {
    if (realUser.user.getAuthenticationMethod() == null) { // if the user's auth method is not set from subject
      switch (authenticationMethod) {                      // set it from configured authenticationMethod
        case TOKEN:
        case CERTIFICATE:
        case KERBEROS_SSL:
        case PROXY:
          throw new UnsupportedOperationException(
              authenticationMethod + " login authentication is not supported");
        default:
          LOG.debug("Found no authentication principals in subject. Simple?");
          realUser.user.setAuthenticationMethod(authenticationMethod);
      }
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public synchronized static void setLoginUser(UserGroupInformation ugi) {
    // if this is to become stable, should probably logout the currently
    // logged in ugi if it's different
    loginUser = ugi;
  }
  
  /**
   * Is this user logged in from a keytab file?
   * @return true if the credentials are from a keytab file.
   */
  public boolean isFromKeytab() {
    return isKeytab;
  }
  
  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  private synchronized KerberosTicket getTGT() {
    Set<KerberosTicket> tickets = subject
        .getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket ticket : tickets) {
      if (SecurityUtil.isOriginalTGT(ticket)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found tgt " + ticket);
        }
        return ticket;
      }
    }
    return null;
  }
  
  private long getRefreshTime(KerberosTicket tgt) {
    long start = tgt.getStartTime().getTime();
    long end = tgt.getEndTime().getTime();
    return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
  }

  /**Spawn a thread to do periodic renewals of kerberos credentials*/
  private void spawnAutoRenewalThreadForUserCreds() {
    if (isSecurityEnabled()) {
      //spawn thread only if we have kerb credentials
      if (user.getAuthenticationMethod() == AuthenticationMethod.KERBEROS &&
          !isKeytab) {
        Thread t = new Thread(new Runnable() {
          
          @Override
          public void run() {
            String cmd = conf.get("hadoop.kerberos.kinit.command",
                                  "kinit");
            KerberosTicket tgt = getTGT();
            if (tgt == null) {
              return;
            }
            long nextRefresh = getRefreshTime(tgt);
            while (true) {
              try {
                long now = Time.now();
                if(LOG.isDebugEnabled()) {
                  LOG.debug("Current time is " + now);
                  LOG.debug("Next refresh is " + nextRefresh);
                }
                if (now < nextRefresh) {
                  Thread.sleep(nextRefresh - now);
                }
                Shell.execCommand(cmd, "-R");
                if(LOG.isDebugEnabled()) {
                  LOG.debug("renewed ticket");
                }
                reloginFromTicketCache();
                tgt = getTGT();
                if (tgt == null) {
                  LOG.warn("No TGT after renewal. Aborting renew thread for " +
                           getUserName());
                  return;
                }
                nextRefresh = Math.max(getRefreshTime(tgt),
                                       now + MIN_TIME_BEFORE_RELOGIN);
              } catch (InterruptedException ie) {
                LOG.warn("Terminating renewal thread");
                return;
              } catch (IOException ie) {
                LOG.warn("Exception encountered while running the" +
                    " renewal command. Aborting renew thread. " + ie);
                return;
              }
            }
          }
        });
        t.setDaemon(true);
        t.setName("TGT Renewer for " + getUserName());
        t.start();
      }
    }
  }
  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException if the keytab file can't be read
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized
  static void loginUserFromKeytab(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return;

    keytabFile = path;
    keytabPrincipal = user;
    Subject subject = new Subject();
    LoginContext login; 
    long start = 0;
    try {
      login = newLoginContextWithKeyTab(serviceJAASConfName,
          subject, keytabFile, keytabPrincipal);
      start = Time.now();
      login.login();
      metrics.loginSuccess.add(Time.now() - start);
      loginUser = new UserGroupInformation(subject, serviceJAASConfName);
      loginUser.setLogin(login);
      // AuthenticationMethod is now computed based upon subject
      // loginUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + user + " from keytab " + 
                            path+ ": " + le, le);
    }
    LOG.info("Login successful for user " + keytabPrincipal
        + " using keytab file " + keytabFile);
  }

  /**
   * Log the current user out who previously logged in using keytab.
   * This method assumes that the user logged in by calling
   * {@link #loginUserFromKeytab(String, String)}.
   *
   * @throws IOException if a failure occurred in logout, or if the user did
   * not log in by invoking loginUserFromKeyTab() before.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void logoutUserFromKeytab() throws IOException {
    if (!isSecurityEnabled() ||
        user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS) {
      return;
    }
    LoginContext login = getLogin();
    if (login == null || keytabFile == null) {
      throw new IOException("loginUserFromKeytab must be done first");
    }

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      synchronized (UserGroupInformation.class) {
        login.logout();
      }
    } catch (LoginException le) {
      throw new IOException("Logout failure for " + user + " from keytab " +
          keytabFile, le);
    }

    LOG.info("Logout successful for user " + keytabPrincipal
        + " using keytab file " + keytabFile);
  }
  
  /**
   * Re-login a user from keytab if TGT is expired or is close to expiry.
   * 
   * @throws IOException
   */
  public synchronized void checkTGTAndReloginFromKeytab() throws IOException {
    if (!isSecurityEnabled()
        || user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS
        || !isKeytab)
      return;
    KerberosTicket tgt = getTGT();
    if (tgt != null && !shouldRenewImmediatelyForTests &&
        Time.now() < getRefreshTime(tgt)) {
      return;
    }
    reloginFromKeytab();
  }

  /**
   * Re-Login a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user. This
   * method assumes that {@link #loginUserFromKeytab(String, String)} had 
   * happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized void reloginFromKeytab()
  throws IOException {
    if (!isSecurityEnabled() ||
         user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS ||
         !isKeytab)
      return;
    
    long now = Time.now();
    if (!shouldRenewImmediatelyForTests && !hasSufficientTimeElapsed(now)) {
      return;
    }

    KerberosTicket tgt = getTGT();
    //Return if TGT is valid and is not going to expire soon.
    if (tgt != null && !shouldRenewImmediatelyForTests &&
        now < getRefreshTime(tgt)) {
      return;
    }
    
    LoginContext login = getLogin();
    if (login == null || keytabFile == null) {
      throw new IOException("loginUserFromKeyTab must be done first");
    }
    
    long start = 0;
    // register most recent relogin attempt
    user.setLastLogin(now);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      synchronized (UserGroupInformation.class) {
        // clear up the kerberos state. But the tokens are not cleared! As per
        // the Java kerberos login module code, only the kerberos credentials
        // are cleared
        login.logout();
        // login and also update the subject field of this instance to
        // have the new credentials (pass it to the LoginContext constructor)
        login = newLoginContextWithKeyTab(serviceJAASConfName, getSubject(),
                                          keytabFile, keytabPrincipal);
        LOG.info("Initiating re-login for " + keytabPrincipal);
        start = Time.now();
        login.login();
        metrics.loginSuccess.add(Time.now() - start);
        setLogin(login);
      }
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + keytabPrincipal + 
          " from keytab " + keytabFile, le);
    } 
  }

  /**
   * Re-Login a user in from the ticket cache.  This
   * method assumes that login had happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized void reloginFromTicketCache()
  throws IOException {
    if (!isSecurityEnabled() || 
        user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS ||
        !isKrbTkt)
      return;
    LoginContext login = getLogin();
    if (login == null) {
      throw new IOException("login must be done first");
    }
    long now = Time.now();
    if (!hasSufficientTimeElapsed(now)) {
      return;
    }
    // register most recent relogin attempt
    user.setLastLogin(now);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      //clear up the kerberos state. But the tokens are not cleared! As per 
      //the Java kerberos login module code, only the kerberos credentials
      //are cleared
      login.logout();
      //login and also update the subject field of this instance to 
      //have the new credentials (pass it to the LoginContext constructor)
      login = newLoginContext(userJAASConfName, getSubject(), null);
      LOG.info("Initiating re-login for " + getUserName());
      login.login();
      setLogin(login);
    } catch (LoginException le) {
      throw new IOException("Login failure for " + getUserName(), le);
    } 
  }


  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and login them in. This new user does not affect the currently
   * logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException if the keytab file can't be read
   */
  public synchronized
  static UserGroupInformation loginUserFromKeytabAndReturnUGI(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return UserGroupInformation.getCurrentUser();
    String oldKeytabFile = null;
    String oldKeytabPrincipal = null;

    long start = 0;
    try {
      oldKeytabFile = keytabFile;
      oldKeytabPrincipal = keytabPrincipal;
      keytabFile = path;
      keytabPrincipal = user;
      Subject subject = new Subject();
      
      LoginContext login = newLoginContextWithKeyTab(serviceJAASConfName, subject,
                                                     keytabFile, keytabPrincipal);
       
      start = Time.now();
      login.login();
      metrics.loginSuccess.add(Time.now() - start);
      UserGroupInformation newLoginUser =
          new UserGroupInformation(subject, serviceJAASConfName);
      newLoginUser.setLogin(login);
      // AuthenticationMethod is now computed based upon subject
      // newLoginUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
      return newLoginUser;
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + user + " from keytab " + 
                            path, le);
    } finally {
      if(oldKeytabFile != null) keytabFile = oldKeytabFile;
      if(oldKeytabPrincipal != null) keytabPrincipal = oldKeytabPrincipal;
    }
  }

  private boolean hasSufficientTimeElapsed(long now) {
    if (now - user.getLastLogin() < MIN_TIME_BEFORE_RELOGIN ) {
      LOG.warn("Not attempting to re-login since the last re-login was " +
          "attempted less than " + (MIN_TIME_BEFORE_RELOGIN/1000) + " seconds"+
          " before.");
      return false;
    }
    return true;
  }
  
  /**
   * Did the login happen via keytab
   * @return true or false
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized static boolean isLoginKeytabBased() throws IOException {
    return getLoginUser().isKeytab;
  }

  /**
   * Did the login happen via ticket cache
   * @return true or false
   */
  public static boolean isLoginTicketBased()  throws IOException {
    return getLoginUser().isKrbTkt;
  }

  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC, since it won't have any credentials.
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroupInformation for the remote user.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createRemoteUser(String user) {
    return createRemoteUser(user, AuthMethod.SIMPLE);
  }
  
  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC, since it won't have any credentials.
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroupInformation for the remote user.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createRemoteUser(String user, AuthMethod authMethod) {
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Null user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    UserGroupInformation result = new UserGroupInformation(subject);
    result.setAuthenticationMethod(authMethod);
    return result;
  }

  /**
   * existing types of authentications' methods
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static enum AuthenticationMethod {
    // currently we support only one auth per method, but eventually a 
    // subtype is needed to differentiate, ex. if digest is token or ldap
    SIMPLE(false),
    KERBEROS(true),
    TOKEN(false),
    CERTIFICATE(true),
    KERBEROS_SSL(true),
    PROXY(false),
    CUSTOM(true);
    
    private final boolean allowsDelegation;
    
    private AuthenticationMethod(boolean allowsDelegation) {
      this.allowsDelegation = allowsDelegation;
    }
    
    public boolean allowsDelegation() {
      return allowsDelegation;
    }
  }

  /**
   * Create a proxy user using username of the effective user and the ugi of the
   * real user.
   * @param user
   * @param realUser
   * @return proxyUser ugi
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createProxyUser(String user,
      UserGroupInformation realUser) {
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Null user");
    }
    if (realUser == null) {
      throw new IllegalArgumentException("Null real user");
    }
    Subject subject = new Subject();
    Set<Principal> principals = subject.getPrincipals();
    principals.add(new User(user));
    principals.add(new RealUser(realUser));
    UserGroupInformation result =new UserGroupInformation(subject);
    result.setAuthenticationMethod(AuthenticationMethod.PROXY);
    return result;
  }

  /**
   * get RealUser (vs. EffectiveUser)
   * @return realUser running over proxy user
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public UserGroupInformation getRealUser() {
    for (RealUser p: subject.getPrincipals(RealUser.class)) {
      return p.getRealUser();
    }
    return null;
  }


  
  /**
   * This class is used for storing the groups for testing. It stores a local
   * map that has the translation of usernames to groups.
   */
  private static class TestingGroups extends Groups {
    private final Map<String, List<String>> userToGroupsMapping = 
      new HashMap<String,List<String>>();
    private Groups underlyingImplementation;
    
    private TestingGroups(Groups underlyingImplementation) {
      super(new org.apache.hadoop.conf.Configuration());
      this.underlyingImplementation = underlyingImplementation;
    }
    
    @Override
    public List<String> getGroups(String user) throws IOException {
      List<String> result = userToGroupsMapping.get(user);
      
      if (result == null) {
        result = underlyingImplementation.getGroups(user);
      }

      return result;
    }

    private void setUserGroups(String user, String[] groups) {
      userToGroupsMapping.put(user, Arrays.asList(groups));
    }
  }

  /**
   * Create a UGI for testing HDFS and MapReduce
   * @param user the full user principal name
   * @param userGroups the names of the groups that the user belongs to
   * @return a fake user for running unit tests
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createUserForTesting(String user, 
                                                          String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createRemoteUser(user);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups(groups);
    }
    // add the user groups
    ((TestingGroups) groups).setUserGroups(ugi.getShortUserName(), userGroups);
    return ugi;
  }


  /**
   * Create a proxy user UGI for testing HDFS and MapReduce
   * 
   * @param user
   *          the full user principal name for effective user
   * @param realUser
   *          UGI of the real user
   * @param userGroups
   *          the names of the groups that the user belongs to
   * @return a fake user for running unit tests
   */
  public static UserGroupInformation createProxyUserForTesting(String user,
      UserGroupInformation realUser, String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createProxyUser(user, realUser);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups(groups);
    }
    // add the user groups
    ((TestingGroups) groups).setUserGroups(ugi.getShortUserName(), userGroups);
    return ugi;
  }
  
  /**
   * Get the user's login name.
   * @return the user's name up to the first '/' or '@'.
   */
  public String getShortUserName() {
    if (windows && spoofUser) {
      return spoofedUser;
    }

    for (User p: subject.getPrincipals(User.class)) {
      return p.getShortName();
    }
    return null;
  }

  public String getPrimaryGroupName() throws IOException {
    String[] groups = getGroupNames();
    if (groups.length == 0) {
      throw new IOException("There is no primary group for UGI " + this);
    }
    return groups[0];
  }

  /**
   * Get the user's full principal name.
   * @return the user's full principal name.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public String getUserName() {
    if (windows && spoofUser) {
      return spoofedUser;
    }

    return user.getName();
  }

  /**
   * Add a TokenIdentifier to this UGI. The TokenIdentifier has typically been
   * authenticated by the RPC layer as belonging to the user represented by this
   * UGI.
   * 
   * @param tokenId
   *          tokenIdentifier to be added
   * @return true on successful add of new tokenIdentifier
   */
  public synchronized boolean addTokenIdentifier(TokenIdentifier tokenId) {
    return subject.getPublicCredentials().add(tokenId);
  }

  /**
   * Get the set of TokenIdentifiers belonging to this UGI
   * 
   * @return the set of TokenIdentifiers belonging to this UGI
   */
  public synchronized Set<TokenIdentifier> getTokenIdentifiers() {
    return subject.getPublicCredentials(TokenIdentifier.class);
  }
  
  /**
   * Add a token to this UGI
   * 
   * @param token Token to be added
   * @return true on successful add of new token
   */
  public boolean addToken(Token<? extends TokenIdentifier> token) {
    return (token != null) ? addToken(token.getService(), token) : false;
  }

  /**
   * Add a named token to this UGI
   * 
   * @param alias Name of the token
   * @param token Token to be added
   * @return true on successful add of new token
   */
  public boolean addToken(Text alias, Token<? extends TokenIdentifier> token) {
    synchronized (subject) {
      getCredentialsInternal().addToken(alias, token);
      return true;
    }
  }
  
  /**
   * Obtain the collection of tokens associated with this user.
   * 
   * @return an unmodifiable collection of tokens associated with user
   */
  public Collection<Token<? extends TokenIdentifier>> getTokens() {
    synchronized (subject) {
      return Collections.unmodifiableCollection(
          new ArrayList<Token<?>>(getCredentialsInternal().getAllTokens()));
    }
  }

  /**
   * Obtain the tokens in credentials form associated with this user.
   * 
   * @return Credentials of tokens associated with this user
   */
  public Credentials getCredentials() {
    synchronized (subject) {
      Credentials creds = new Credentials(getCredentialsInternal());
      Iterator<Token<?>> iter = creds.getAllTokens().iterator();
      while (iter.hasNext()) {
        if (iter.next() instanceof Token.PrivateToken) {
          iter.remove();
        }
      }
      return creds;
    }
  }
  
  /**
   * Add the given Credentials to this user.
   * @param credentials of tokens and secrets
   */
  public void addCredentials(Credentials credentials) {
    synchronized (subject) {
      getCredentialsInternal().addAll(credentials);
    }
  }

  private synchronized Credentials getCredentialsInternal() {
    final Credentials credentials;
    final Set<Credentials> credentialsSet =
      subject.getPrivateCredentials(Credentials.class);
    if (!credentialsSet.isEmpty()){
      credentials = credentialsSet.iterator().next();
    } else {
      credentials = new Credentials();
      subject.getPrivateCredentials().add(credentials);
    }
    return credentials;
  }

  /**
   * Get the group names for this user.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public synchronized String[] getGroupNames() {
    ensureInitialized();
    try {
      Set<String> result = new LinkedHashSet<String>
        (groups.getGroups(getShortUserName()));
      return result.toArray(new String[result.size()]);
    } catch (IOException ie) {
      LOG.warn("No groups available for user " + getShortUserName());
      return new String[0];
    }
  }
  
  /**
   * Return the username.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getUserName());
    sb.append(" (auth:"+getAuthenticationMethod()+")");
    if (getRealUser() != null) {
      sb.append(" via ").append(getRealUser().toString());
    }
    return sb.toString();
  }

  /**
   * Sets the authentication method in the subject
   * 
   * @param authMethod
   */
  public synchronized 
  void setAuthenticationMethod(AuthenticationMethod authMethod) {
    user.setAuthenticationMethod(authMethod);
  }

  /**
   * Sets the authentication method in the subject
   * 
   * @param authMethod
   */
  //TODO: Delete this method once AuthMethod is deprecated.
  public void setAuthenticationMethod(AuthMethod authMethod) {
    switch (authMethod) {
      case SIMPLE:
        user.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
        break;
      case KERBEROS: {
        user.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
        break;
      }
      case DIGEST:
      case TOKEN: {
        user.setAuthenticationMethod(AuthenticationMethod.TOKEN);
        break;
      }
      default:
        user.setAuthenticationMethod(null);
    }
  }

  /**
   * Get the authentication method from the subject
   * 
   * @return AuthenticationMethod in the subject, null if not present.
   */
  public synchronized AuthenticationMethod getAuthenticationMethod() {
    return user.getAuthenticationMethod();
  }

  /**
   * Get the authentication method from the real user's subject.  If there
   * is no real user, return the given user's authentication method.
   * 
   * @return AuthenticationMethod in the subject, null if not present.
   */
  public synchronized AuthenticationMethod getRealAuthenticationMethod() {
    UserGroupInformation ugi = getRealUser();
    if (ugi == null) {
      ugi = this;
    }
    return ugi.getAuthenticationMethod();
  }

  /**
   * Returns the authentication method of a ugi. If the authentication method is
   * PROXY, returns the authentication method of the real user.
   * 
   * @param ugi
   * @return AuthenticationMethod
   */
  public static AuthenticationMethod getRealAuthenticationMethod(
      UserGroupInformation ugi) {
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   * Compare the subjects to see if they are equal to each other.
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return subject == ((UserGroupInformation) o).subject;
    }
  }

  /**
   * Return the hash of the subject.
   */
  @Override
  public int hashCode() {
    return System.identityHashCode(subject);
  }

  /**
   * Get the underlying subject from this ugi.
   * @return the subject that represents this user.
   */
  public Subject getSubject() {
    return subject;
  }

  /**
   * Run the given action as the user.
   * @param <T> the return type of the run method
   * @param action the method to execute
   * @return the value from the run method
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public <T> T doAs(PrivilegedAction<T> action) {
    logPrivilegedAction(subject, action);
    return Subject.doAs(subject, action);
  }
  
  /**
   * Run the given action as the user, potentially throwing an exception.
   * @param <T> the return type of the run method
   * @param action the method to execute
   * @return the value from the run method
   * @throws IOException if the action throws an IOException
   * @throws Error if the action throws an Error
   * @throws RuntimeException if the action throws a RuntimeException
   * @throws InterruptedException if the action throws an InterruptedException
   * @throws UndeclaredThrowableException if the action throws something else
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public <T> T doAs(PrivilegedExceptionAction<T> action
                    ) throws IOException, InterruptedException {
    try {
      logPrivilegedAction(subject, action);
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      if (LOG.isDebugEnabled()) {
        LOG.debug("PrivilegedActionException as:" + this + " cause:" + cause);
      }
      if (cause == null) {
        throw new RuntimeException("PrivilegedActionException with no " +
                "underlying cause. UGI [" + this + "]", pae);
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else {
        throw new UndeclaredThrowableException(cause);
      }
    }
  }

  private void logPrivilegedAction(Subject subject, Object action) {
    if (LOG.isDebugEnabled()) {
      // would be nice if action included a descriptive toString()
      String where = new Throwable().getStackTrace()[2].toString();
      LOG.debug("PrivilegedAction as:"+this+" from:"+where);
    }
  }

  private void print() throws IOException {
    System.out.println("User: " + getUserName());
    System.out.print("Group Ids: ");
    System.out.println();
    String[] groups = getGroupNames();
    System.out.print("Groups: ");
    for(int i=0; i < groups.length; i++) {
      System.out.print(groups[i] + " ");
    }
    System.out.println();    
  }

  /**
   * A test method to print out the current user's UGI.
   * @param args if there are two arguments, read the user from the keytab
   * and print it out.
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
  System.out.println("Getting UGI for current user");
    UserGroupInformation ugi = getCurrentUser();
    ugi.print();
    System.out.println("UGI: " + ugi);
    System.out.println("Auth method " + ugi.user.getAuthenticationMethod());
    System.out.println("Keytab " + ugi.isKeytab);
    System.out.println("============================================================");
    
    if (args.length == 2) {
      System.out.println("Getting UGI from keytab....");
      loginUserFromKeytab(args[0], args[1]);
      getCurrentUser().print();
      System.out.println("Keytab: " + ugi);
      System.out.println("Auth method " + loginUser.user.getAuthenticationMethod());
      System.out.println("Keytab " + loginUser.isKeytab);
    }
  }

}
