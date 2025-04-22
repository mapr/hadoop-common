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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.FileSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerException;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;


/**
 * The {@link AuthenticationFilter} enables protecting web application
 * resources with different (pluggable)
 * authentication mechanisms and signer secret providers.
 * <p>
 * Additional authentication mechanisms are supported via the {@link AuthenticationHandler} interface.
 * <p>
 * This filter delegates to the configured authentication handler for authentication and once it obtains an
 * {@link AuthenticationToken} from it, sets a signed HTTP cookie with the token. For client requests
 * that provide the signed HTTP cookie, it verifies the validity of the cookie, extracts the user information
 * and lets the request proceed to the target resource.
 * <p>
 * The rest of the configuration properties are specific to the {@link AuthenticationHandler} implementation and the
 * {@link AuthenticationFilter} will take all the properties that start with the prefix #PREFIX#, it will remove
 * the prefix from it and it will pass them to the the authentication handler for initialization. Properties that do
 * not start with the prefix will not be passed to the authentication handler initialization.
 * <p>
 * Details of the configurations are listed on <a href="../../../../../../../Configuration.html">Configuration Page</a>
 * <p>
 * The "zookeeper" implementation has additional configuration properties that
 * must be specified; see {@link ZKSignerSecretProvider} for details.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AuthenticationFilter implements Filter {

  private static Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

  /**
   * Constant for the property that specifies the configuration prefix.
   */
  public static final String CONFIG_PREFIX = "config.prefix";

  /**
   * Constant for the property that specifies the authentication handler to use.
   */
  public static final String AUTH_TYPE = "type";

  /**
   * Constant for the property that specifies the secret to use for signing the HTTP Cookies.
   */
  public static final String SIGNATURE_SECRET = "signature.secret";

  public static final String SIGNATURE_SECRET_FILE = SIGNATURE_SECRET + ".file";

  /**
   * Constant for the configuration property
   * that indicates the max inactive interval of the generated token.
   */
  public static final String
      AUTH_TOKEN_MAX_INACTIVE_INTERVAL = "token.max-inactive-interval";

  /**
   * Constant for the configuration property that indicates the validity of the generated token.
   */
  public static final String AUTH_TOKEN_VALIDITY = "token.validity";

  /**
   * Constant for the configuration property that indicates the domain to use in the HTTP cookie.
   */
  public static final String COOKIE_DOMAIN = "cookie.domain";

  /**
   * Constant for the configuration property that indicates the path to use in the HTTP cookie.
   */
  public static final String COOKIE_PATH = "cookie.path";

  /**
   * Constant for the configuration property
   * that indicates the persistence of the HTTP cookie.
   */
  public static final String COOKIE_PERSISTENT = "cookie.persistent";

  /**
   * Constant for the configuration property that indicates the name of the
   * SignerSecretProvider class to use.
   * Possible values are: "file", "random", "zookeeper", or a classname.
   * If not specified, the "file" implementation will be used with
   * SIGNATURE_SECRET_FILE; and if that's not specified, the "random"
   * implementation will be used.
   */
  public static final String SIGNER_SECRET_PROVIDER =
      "signer.secret.provider";

  /**
   * Constant for the ServletContext attribute that can be used for providing a
   * custom implementation of the SignerSecretProvider. Note that the class
   * should already be initialized. If not specified, SIGNER_SECRET_PROVIDER
   * will be used.
   */
  public static final String SIGNER_SECRET_PROVIDER_ATTRIBUTE =
      "signer.secret.provider.object";

  public static final String SSO_LOGIN_COOKIE = "isSsoLogin";
  public static final String ACTION_PARAM = "action";


  private Properties config;
  private Signer signer;
  private SignerSecretProvider secretProvider;
  private AuthenticationHandler authHandler;
  private long maxInactiveInterval;
  private long validity;
  private String cookieDomain;
  private String cookiePath;
  private boolean isCookiePersistent;
  private boolean destroySecretProvider;
  /**
   * Configuration prefix value for properties.
   */
  protected String configPrefix;

  /**
   * <p>Initializes the authentication filter and signer secret provider.</p>
   * It instantiates and initializes the specified {@link
   * AuthenticationHandler}.
   *
   * @param filterConfig filter configuration.
   *
   * @throws ServletException thrown if the filter or the authentication handler could not be initialized properly.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    configPrefix = filterConfig.getInitParameter(CONFIG_PREFIX);
    configPrefix = (configPrefix != null) ? configPrefix + "." : "";
    config = getConfiguration(configPrefix, filterConfig);
    String authHandlerName = config.getProperty(AUTH_TYPE, null);
    String authHandlerClassName;
    if (authHandlerName == null) {
      throw new ServletException("Authentication type must be specified: " +
          PseudoAuthenticationHandler.TYPE + "|" +
          KerberosAuthenticationHandler.TYPE + "|<class>");
    }
    authHandlerClassName =
        AuthenticationHandlerUtil
            .getAuthenticationHandlerClassName(authHandlerName);
    maxInactiveInterval = Long.parseLong(config.getProperty(
        AUTH_TOKEN_MAX_INACTIVE_INTERVAL, "-1")); // By default, disable.
    if (maxInactiveInterval > 0) {
      maxInactiveInterval *= 1000;
    }
    validity = Long.parseLong(config.getProperty(AUTH_TOKEN_VALIDITY, "36000"))
        * 1000; //10 hours
    initializeSecretProvider(filterConfig);

    initializeAuthHandler(authHandlerClassName, filterConfig);

    String domainName = null;
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      String fqdn = localHost.getCanonicalHostName();
      if (fqdn != null && !fqdn.isEmpty() && fqdn.contains(".")) {
        domainName = fqdn.substring(fqdn.indexOf("."));
      }
    } catch (UnknownHostException e) {
    }

    cookieDomain = config.getProperty(COOKIE_DOMAIN, domainName);
    cookiePath = config.getProperty(COOKIE_PATH, "/");
    isCookiePersistent = Boolean.parseBoolean(
        config.getProperty(COOKIE_PERSISTENT, "false"));

  }

  protected void initializeAuthHandler(String authHandlerClassName, FilterConfig filterConfig)
      throws ServletException {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(authHandlerClassName);
      authHandler = (AuthenticationHandler) klass.newInstance();
      authHandler.init(config);
    } catch (ClassNotFoundException | InstantiationException |
             IllegalAccessException ex) {
      throw new ServletException(ex);
    }
  }

  protected void initializeSecretProvider(FilterConfig filterConfig)
      throws ServletException {
    secretProvider = (SignerSecretProvider) filterConfig.getServletContext().
        getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
    if (secretProvider == null) {
      // As tomcat cannot specify the provider object in the configuration.
      // It'll go into this path
      try {
        secretProvider = constructSecretProvider(
            filterConfig.getServletContext(),
            config, false);
        destroySecretProvider = true;
      } catch (Exception ex) {
        throw new ServletException(ex);
      }
    }
    signer = new Signer(secretProvider);
  }

  public static SignerSecretProvider constructSecretProvider(
      ServletContext ctx, Properties config,
      boolean disallowFallbackToRandomSecretProvider) throws Exception {
    String name = config.getProperty(SIGNER_SECRET_PROVIDER, "file");
    long validity = Long.parseLong(config.getProperty(AUTH_TOKEN_VALIDITY,
        "36000")) * 1000;

    if (!disallowFallbackToRandomSecretProvider
        && "file".equals(name)
        && config.getProperty(SIGNATURE_SECRET_FILE) == null) {
      name = "random";
    }

    SignerSecretProvider provider;
    if ("file".equals(name)) {
      provider = new FileSignerSecretProvider();
      try {
        provider.init(config, ctx, validity);
      } catch (Exception e) {
        if (!disallowFallbackToRandomSecretProvider) {
          LOG.warn("Unable to initialize FileSignerSecretProvider, " +
              "falling back to use random secrets. Reason: " + e.getMessage());
          provider = new RandomSignerSecretProvider();
          provider.init(config, ctx, validity);
        } else {
          throw e;
        }
      }
    } else if ("random".equals(name)) {
      provider = new RandomSignerSecretProvider();
      provider.init(config, ctx, validity);
    } else if ("zookeeper".equals(name)) {
      provider = new ZKSignerSecretProvider();
      provider.init(config, ctx, validity);
    } else {
      provider = (SignerSecretProvider) Thread.currentThread().
          getContextClassLoader().loadClass(name).newInstance();
      provider.init(config, ctx, validity);
    }
    return provider;
  }

  /**
   * Returns the configuration properties of the {@link AuthenticationFilter}
   * without the prefix. The returned properties are the same that the
   * {@link #getConfiguration(String, FilterConfig)} method returned.
   *
   * @return the configuration properties.
   */
  protected Properties getConfiguration() {
    return config;
  }

  /**
   * Returns the authentication handler being used.
   *
   * @return the authentication handler being used.
   */
  protected AuthenticationHandler getAuthenticationHandler() {
    return authHandler;
  }

  /**
   * Returns if a random secret is being used.
   *
   * @return if a random secret is being used.
   */
  protected boolean isRandomSecret() {
    return secretProvider.getClass() == RandomSignerSecretProvider.class;
  }

  /**
   * Returns if a custom implementation of a SignerSecretProvider is being used.
   *
   * @return if a custom implementation of a SignerSecretProvider is being used.
   */
  protected boolean isCustomSignerSecretProvider() {
    Class<?> clazz = secretProvider.getClass();
    return clazz != FileSignerSecretProvider.class && clazz !=
        RandomSignerSecretProvider.class && clazz != ZKSignerSecretProvider
        .class;
  }

  /**
   * Returns the max inactive interval time of the generated tokens.
   *
   * @return the max inactive interval time of the generated tokens in seconds.
   */
  protected long getMaxInactiveInterval() {
    return maxInactiveInterval / 1000;
  }

  /**
   * Returns the validity time of the generated tokens.
   *
   * @return the validity time of the generated tokens, in seconds.
   */
  protected long getValidity() {
    return validity / 1000;
  }

  /**
   * Returns the cookie domain to use for the HTTP cookie.
   *
   * @return the cookie domain to use for the HTTP cookie.
   */
  protected String getCookieDomain() {
    return cookieDomain;
  }

  /**
   * Returns the cookie path to use for the HTTP cookie.
   *
   * @return the cookie path to use for the HTTP cookie.
   */
  protected String getCookiePath() {
    return cookiePath;
  }

  /**
   * Returns the cookie persistence to use for the HTTP cookie.
   *
   * @return the cookie persistence to use for the HTTP cookie.
   */
  protected boolean isCookiePersistent() {
    return isCookiePersistent;
  }

  /**
   * Destroys the filter.
   * <p>
   * It invokes the {@link AuthenticationHandler#destroy()} method to release any resources it may hold.
   */
  @Override
  public void destroy() {
    if (authHandler != null) {
      authHandler.destroy();
      authHandler = null;
    }
    if (secretProvider != null && destroySecretProvider) {
      secretProvider.destroy();
      secretProvider = null;
    }
  }

  /**
   * Returns the filtered configuration (only properties starting with the specified prefix). The property keys
   * are also trimmed from the prefix. The returned {@link Properties} object is used to initialized the
   * {@link AuthenticationHandler}.
   * <p>
   * This method can be overriden by subclasses to obtain the configuration from other configuration source than
   * the web.xml file.
   *
   * @param configPrefix configuration prefix to use for extracting configuration properties.
   * @param filterConfig filter configuration object
   *
   * @return the configuration to be used with the {@link AuthenticationHandler} instance.
   *
   * @throws ServletException thrown if the configuration could not be created.
   */
  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
    Properties props = new Properties();
    Enumeration<?> names = filterConfig.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(configPrefix)) {
        String value = filterConfig.getInitParameter(name);
        props.put(name.substring(configPrefix.length()), value);
      }
    }
    return props;
  }

  /**
   * Returns the full URL of the request including the query string.
   * <p>
   * Used as a convenience method for logging purposes.
   *
   * @param request the request object.
   *
   * @return the full URL of the request including the query string.
   */
  protected String getRequestURL(HttpServletRequest request) {
    StringBuffer sb = request.getRequestURL();
    if (request.getQueryString() != null) {
      sb.append("?").append(request.getQueryString());
    }
    return sb.toString();
  }

  /**
   * Returns the {@link AuthenticationToken} for the request.
   * <p>
   * It looks at the received HTTP cookies and extracts the value of the {@link AuthenticatedURL#AUTH_COOKIE}
   * if present. It verifies the signature and if correct it creates the {@link AuthenticationToken} and returns
   * it.
   * <p>
   * If this method returns <code>null</code> the filter will invoke the configured {@link AuthenticationHandler}
   * to perform user authentication.
   *
   * @param request request object.
   *
   * @return the Authentication token if the request is authenticated, <code>null</code> otherwise.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if the token is invalid or if it has expired.
   */
  protected AuthenticationToken getToken(HttpServletRequest request) throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String tokenStr = null;
    Cookie[] cookies = request.getCookies();
    boolean isJWTBasedToken = false;
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(getCookieTokenName())) {
          tokenStr = cookie.getValue();
          if (tokenStr.isEmpty()) {
            throw new AuthenticationException("Unauthorized access");
          }
          try {
            tokenStr = signer.verifyAndExtract(tokenStr);
          } catch (SignerException ex) {
            throw new AuthenticationException(ex);
          }
        }
        if (cookie.getName().equals(SSO_LOGIN_COOKIE) && cookie.getValue().equals("1")) {
          isJWTBasedToken = true;
        }
      }
    }
    if (tokenStr != null) {
      token = AuthenticationToken.parse(tokenStr);
      boolean match = verifyTokenType(getAuthenticationHandler(), token);
      if (!match) {
        throw new AuthenticationException("Invalid AuthenticationToken type");
      }
      if (token.isExpired()) {
        throw new AuthenticationException("AuthenticationToken expired");
      }
      if(isJWTBasedToken){
        token.setJWTBasedToken(true);
      }
    }
    return token;
  }

  /**
   * This method verifies if the specified token type matches one of the the
   * token types supported by a specified {@link AuthenticationHandler}. This
   * method is specifically designed to work with
   * {@link CompositeAuthenticationHandler} implementation which supports
   * multiple authentication schemes while the {@link AuthenticationHandler}
   * interface supports a single type via
   * {@linkplain AuthenticationHandler#getType()} method.
   *
   * @param handler The authentication handler whose supported token types
   *                should be used for verification.
   * @param token   The token whose type needs to be verified.
   * @return true   If the token type matches one of the supported token types
   *         false  Otherwise
   */
  protected boolean verifyTokenType(AuthenticationHandler handler,
                                    AuthenticationToken token) {
    if (!(handler instanceof CompositeAuthenticationHandler)) {
      return handler.getType().equals(token.getType());
    }
    boolean match = false;
    Collection<String> tokenTypes =
        ((CompositeAuthenticationHandler) handler).getTokenTypes();
    for (String tokenType : tokenTypes) {
      if (tokenType.equals(token.getType())) {
        match = true;
        break;
      }
    }
    return match;
  }

  /**
   * If the request has a valid authentication token it allows the request to continue to the target resource,
   * otherwise it triggers an authentication sequence using the configured {@link AuthenticationHandler}.
   *
   * @param request the request object.
   * @param response the response object.
   * @param filterChain the filter chain object.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  @Override
  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain filterChain)
      throws IOException, ServletException {
    boolean unauthorizedResponse = true;
    int errCode = HttpServletResponse.SC_UNAUTHORIZED;
    AuthenticationException authenticationEx = null;
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    boolean isHttps = "https".equals(httpRequest.getScheme());
    if (checkSsoEnableRequest(httpRequest)) {
      if (SsoConfigurationUtil.getInstance().getClientIssuer().isEmpty()) {
        httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        httpResponse.setStatus(HttpServletResponse.SC_OK);
      }
      return;
    }
    try {
      boolean newToken = false;
      AuthenticationToken token;
      try {
        token = getToken(httpRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got token {} from httpRequest {}", token,
              getRequestURL(httpRequest));
        }
      } catch (AuthenticationException ex) {
        LOG.warn("AuthenticationToken ignored: " + ex.getMessage());
        // will be sent back in a 401 unless filter authenticates
        authenticationEx = ex;
        token = null;
      }
      if (authHandler.managementOperation(token, httpRequest, httpResponse)) {
        if (token == null) {
          if (checkRedirectToLogin(httpRequest)) {
            httpResponse.addHeader(HttpHeaders.LOCATION, getLoginURL(httpRequest));
            httpResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
            return;
          } else if (httpRequest.getParameter(ACTION_PARAM) != null
              && httpRequest.getParameter(ACTION_PARAM).equalsIgnoreCase("initSso")) {
            Map<String, String> ssoMap = new HashMap<>();
            ssoMap.put("loginURL", constructLoginURL(httpRequest));
            JSONObject ssoJson = new JSONObject(ssoMap);
            httpResponse.setContentType("application/json");
            httpResponse.getWriter().write(ssoJson.toJSONString());
            httpResponse.setStatus(HttpServletResponse.SC_OK);
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Request [{}] triggering authentication. handler: {}",
                getRequestURL(httpRequest), authHandler.getClass());
          }
          token = authHandler.authenticate(httpRequest, httpResponse);
          if (token != null && token != AuthenticationToken.ANONYMOUS) {
            if (token.getMaxInactives() > 0) {
              token.setMaxInactives(System.currentTimeMillis()
                  + getMaxInactiveInterval() * 1000);
            }
            if (token.getExpires() != 0) {
              token.setExpires(System.currentTimeMillis()
                  + getValidity() * 1000);
            }
          }
          newToken = true;
        }
        if (token != null) {
          if (request.getParameter(ACTION_PARAM) != null && request.getParameter(ACTION_PARAM).equals("logout")) {
            Cookie ssoCookie = null;
            if (token.isJWTBasedToken()) {
              ssoCookie = new Cookie(SSO_LOGIN_COOKIE, "");
              ssoCookie.setMaxAge(0);
              ssoCookie.setPath(getCookiePath());
            }
            Cookie cleanAuthCookie = new Cookie(getCookieTokenName(), "");
            cleanAuthCookie.setMaxAge(0);
            cleanAuthCookie.setPath(getCookiePath());
            String domain = getCookieDomain();
            if (domain != null && httpRequest.getRequestURL().toString().contains(domain)) {
              if (domain.startsWith(".")) {
                domain = domain.substring(1);
              }
              if (ssoCookie != null) {
                ssoCookie.setDomain(domain);
              }
              cleanAuthCookie.setDomain(domain);
            }
            if (ssoCookie != null) {
              httpResponse.addCookie(ssoCookie);
            }
            httpResponse.addCookie(cleanAuthCookie);
            if (token.isJWTBasedToken()) {
              httpResponse.addHeader(HttpHeaders.LOCATION, getLogoutUrl());
              httpResponse.setStatus(HttpServletResponse.SC_OK);
            } else {
              httpResponse.addHeader(HttpHeaders.LOCATION, getLoginURL(httpRequest));
              httpResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
            }
            return;
          }
          unauthorizedResponse = false;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Request [{}] user [{}] authenticated",
                getRequestURL(httpRequest), token.getUserName());
          }
          final AuthenticationToken authToken = token;
          httpRequest = new HttpServletRequestWrapper(httpRequest) {

            @Override
            public String getAuthType() {
              return authToken.getType();
            }

            @Override
            public String getRemoteUser() {
              return authToken.getUserName();
            }

            @Override
            public Principal getUserPrincipal() {
              return (authToken != AuthenticationToken.ANONYMOUS) ?
                  authToken : null;
            }
          };

          // If cookie persistence is configured to false,
          // it means the cookie will be a session cookie.
          // If the token is an old one, renew the its maxInactiveInterval.
          if (!newToken && !isCookiePersistent()
              && getMaxInactiveInterval() > 0) {
            token.setMaxInactives(System.currentTimeMillis()
                + getMaxInactiveInterval() * 1000);
            token.setExpires(token.getExpires());
            newToken = true;
          }
          if (newToken && !token.isExpired()
              && token != AuthenticationToken.ANONYMOUS) {
            String signedToken = signer.sign(token.toString());
            createAuthCookie(getCookieTokenName(), httpResponse, httpRequest, signedToken, getCookieDomain(),
                getCookiePath(), token.getExpires(),
                isCookiePersistent(), isHttps, token.getJWTExpires());
            if(token.isJWTBasedToken()){
              createSSOLoginCookie(httpResponse, httpRequest, getCookieDomain(),
                  getCookiePath(), token.getExpires(), isHttps, token.getJWTExpires());
            }
          }
          doFilter(filterChain, httpRequest, httpResponse);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("managementOperation returned false for request {}."
              + " token: {}", getRequestURL(httpRequest), token);
        }
        unauthorizedResponse = false;
      }
    } catch (AuthenticationException ex) {
      // exception from the filter itself is fatal
      errCode = HttpServletResponse.SC_FORBIDDEN;
      authenticationEx = ex;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Authentication exception: " + ex.getMessage(), ex);
      } else {
        LOG.warn("Authentication exception: " + ex.getMessage());
      }
    }
    if (unauthorizedResponse) {
      if (!httpResponse.isCommitted()) {
        createAuthCookie(getCookieTokenName(), httpResponse, httpRequest, "", getCookieDomain(),
            getCookiePath(), 0, isCookiePersistent(), isHttps, -1);
        // If response code is 401. Then WWW-Authenticate Header should be
        // present.. reset to 403 if not found..
        if ((errCode == HttpServletResponse.SC_UNAUTHORIZED)
            && (!httpResponse.containsHeader(
            KerberosAuthenticator.WWW_AUTHENTICATE)
            && !httpResponse.containsHeader(
            KerberosAuthenticator.WWW_AUTHENTICATE.toLowerCase()))) {
          errCode = HttpServletResponse.SC_FORBIDDEN;
        }
        // After Jetty 9.4.21, sendError() no longer allows a custom message.
        // use setStatus() to set a custom message.
        String reason;
        if (authenticationEx == null) {
          reason = "Authentication required";
        } else {
          reason = authenticationEx.getMessage();
        }

        httpResponse.setStatus(errCode, reason);
        httpResponse.sendError(errCode, reason);
      }
    }
  }

  private boolean checkSsoEnableRequest(HttpServletRequest httpRequest) {
    return httpRequest.getHeader(HttpHeaders.USER_AGENT).startsWith("Mozilla/5.0") &&
        httpRequest.getParameter(ACTION_PARAM) != null &&
        httpRequest.getParameter(ACTION_PARAM).equalsIgnoreCase("ssoEnable");
  }

  private boolean checkRedirectToLogin(HttpServletRequest httpRequest) {
    return httpRequest.getHeader(KerberosAuthenticator.AUTHORIZATION) == null &&
        httpRequest.getHeader(HttpHeaders.USER_AGENT).startsWith("Mozilla/5.0") &&
        !httpRequest.getRequestURI().replace("/", "").equals("login") &&
        httpRequest.getParameter(ACTION_PARAM) == null;
  }

  private void createSSOLoginCookie(HttpServletResponse res, HttpServletRequest req, String cookieDomain, String cookiePath,
                                    long expires, boolean isHttps, long jwtExpires) {
    Cookie ssoLoginCookie = new Cookie(SSO_LOGIN_COOKIE, "1");
    if (cookieDomain != null &&
        (req == null || req.getRequestURL().toString().contains(cookieDomain))) {
      if(cookieDomain.startsWith(".")){
        cookieDomain = cookieDomain.substring(1);
      }
      ssoLoginCookie.setDomain(cookieDomain);
    }
    if(cookiePath != null){
      ssoLoginCookie.setPath(cookiePath);
    }
    if( expires >= 0 || jwtExpires >= 0){
      long exp = Math.min(expires, jwtExpires);
      int maxAge = (int) (exp - System.currentTimeMillis()) / 1000;
      ssoLoginCookie.setMaxAge(maxAge);
    }
    if(isHttps){
      ssoLoginCookie.setSecure(true);
    }
    ssoLoginCookie.setHttpOnly(true);
    res.addCookie(ssoLoginCookie);
  }

  /**
   * Delegates call to the servlet filter chain. Sub-classes my override this
   * method to perform pre and post tasks.
   *
   * @param filterChain the filter chain object.
   * @param request the request object.
   * @param response the response object.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
    filterChain.doFilter(request, response);
  }

  /**
   * Creates the Hadoop authentication HTTP cookie.
   *
   * @param resp the response object.
   * @param token authentication token for the cookie.
   * @param domain the cookie domain.
   * @param path the cookie path.
   * @param expires UNIX timestamp that indicates the expire date of the
   *                cookie. It has no effect if its value &lt; 0.
   * @param isSecure is the cookie secure?
   * @param isCookiePersistent whether the cookie is persistent or not.
   *
   * XXX the following code duplicate some logic in Jetty / Servlet API,
   * because of the fact that Hadoop is stuck at servlet 2.5 and jetty 6
   * right now.
   */
  public static void createAuthCookie(String name, HttpServletResponse resp, HttpServletRequest req, String token,
                                      String domain, String path, long expires,
                                      boolean isCookiePersistent,
                                      boolean isSecure, long jwtExp) {
    StringBuilder sb = new StringBuilder(name)
        .append("=");

    if (token != null && token.length() > 0) {
      sb.append("\"").append(token).append("\"");
    }

    if (path != null) {
      sb.append("; Path=").append(path);
    }

    if (domain != null &&
        (req == null || req.getRequestURL().toString().contains(domain))) {
      if(domain.startsWith(".")){
        domain = domain.substring(1);
      }
      sb.append("; Domain=").append(domain);
    }
    if ((expires >= 0 && isCookiePersistent) || jwtExp >= 0) {
      Date date;
      if (jwtExp < expires && jwtExp >= 0) {
        date = new Date(jwtExp);
      } else {
        date = new Date(expires);
      }
      SimpleDateFormat df = new SimpleDateFormat("EEE, " +
          "dd-MMM-yyyy HH:mm:ss zzz", Locale.US);
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      sb.append("; Expires=").append(df.format(date));
    }

    if (isSecure) {
      sb.append("; Secure");
    }

    sb.append("; HttpOnly");
    resp.addHeader("Set-Cookie", sb.toString());
  }

  @VisibleForTesting
  String constructLoginURL(HttpServletRequest request) {
    String delimiter = "&";
    String ui2 = request.getParameter("ui2") != null ? "ui2" : "";
    return getAuthUrl() + "?" +
        "response_type=code" + delimiter + "client_id=" + SsoConfigurationUtil.getInstance().getClientId() + delimiter + "scope=openid" + delimiter +
        "redirect_uri=" + JWTUtils.constructURLWithHostname(request.getRequestURL().toString()) + ui2 + "?" + ACTION_PARAM + "=processCode";
  }

  public String getAuthUrl() {
    return SsoConfigurationUtil.getInstance().getClientIssuer() + "/protocol/openid-connect/auth";
  }

  public String getLogoutUrl() {
    return SsoConfigurationUtil.getInstance().getClientIssuer() + "/protocol/openid-connect/logout";
  }

  private String getLoginURL(HttpServletRequest httpRequest){
    String redirect;
    try {
      URI origin = new URI(httpRequest.getRequestURL().toString());
      redirect = origin.getScheme() + "://" + origin.getHost() + ":" + origin.getPort() + "/login";
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return redirect;
  }
  public String getCookieTokenName(){
    return AuthenticatedURL.AUTH_COOKIE;
  }
}
