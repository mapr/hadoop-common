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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.webproxy.ProxyUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Public
public class AmIpFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(AmIpFilter.class);
  
  @Deprecated
  public static final String PROXY_HOST = "PROXY_HOST";
  @Deprecated
  public static final String PROXY_URI_BASE = "PROXY_URI_BASE";
  public static final String PROXY_HOSTS = "PROXY_HOSTS";
  public static final String PROXY_HOSTS_DELIMITER = ",";
  public static final String PROXY_URI_BASES = "PROXY_URI_BASES";
  public static final String PROXY_URI_BASES_DELIMITER = ",";
  private static final String PROXY_PATH = "/proxy";
  //update the proxy IP list about every 5 min
  private static final long UPDATE_INTERVAL = 5 * 60 * 1000;

  private List<String> proxyHosts = new ArrayList<>();
  private Set<String> proxyAddresses = null;
  private long lastUpdate;
  private Map<String, String> proxyUriBases;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    // Maintain for backwards compatibility
    if (conf.getInitParameter(PROXY_HOST) != null
        && conf.getInitParameter(PROXY_URI_BASE) != null) {
      //proxyHosts = new ArrayList<>(1);
      proxyHosts.add(conf.getInitParameter(PROXY_HOST));
      proxyUriBases = new HashMap<>(1);
      proxyUriBases.put("dummy", conf.getInitParameter(PROXY_URI_BASE));
    } else {
      proxyHosts.addAll(Arrays.asList(conf.getInitParameter(PROXY_HOSTS)
          .split(PROXY_HOSTS_DELIMITER)));

      String[] proxyUriBasesArr = conf.getInitParameter(PROXY_URI_BASES)
          .split(PROXY_URI_BASES_DELIMITER);
      proxyUriBases = new HashMap<>(proxyUriBasesArr.length);
      for (String proxyUriBase : proxyUriBasesArr) {
        try {
          URL url = new URL(proxyUriBase);
          proxyUriBases.put(url.getHost() + ":" + url.getPort(), proxyUriBase);
        } catch(MalformedURLException e) {
          LOG.warn("{} does not appear to be a valid URL", proxyUriBase, e);
        }
      }
    }
  }

  protected Set<String> getProxyAddresses() throws ServletException {
    long now = System.currentTimeMillis();
    synchronized(this) {
      if (proxyAddresses == null || (lastUpdate + UPDATE_INTERVAL) >= now) {
        proxyAddresses = new HashSet<>();
        for (String proxyHost : proxyHosts) {
          try {
              for(InetAddress add : InetAddress.getAllByName(proxyHost)) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("proxy address is: {}", add.getHostAddress());
                }
                proxyAddresses.add(add.getHostAddress());
              }
              lastUpdate = now;
            } catch (UnknownHostException e) {
              LOG.warn("Could not locate {} - skipping", proxyHost, e);
            }
          }
        if (proxyAddresses.isEmpty()) {
          throw new ServletException("Could not locate any of the proxy hosts");
        }
      }
      return proxyAddresses;
    }
  }

  @Override
  public void destroy() {
    //Empty
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp,
      FilterChain chain) throws IOException, ServletException {
    ProxyUtils.rejectNonHttpRequests(req);

    HttpServletRequest httpReq = (HttpServletRequest)req;
    HttpServletResponse httpResp = (HttpServletResponse)resp;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Remote address for request is: {}", httpReq.getRemoteAddr());
    }

    if (!getProxyAddresses().contains(httpReq.getRemoteAddr())) {
      StringBuilder redirect = new StringBuilder(findRedirectUrl());

      redirect.append(httpReq.getRequestURI());

      int insertPoint = redirect.indexOf(PROXY_PATH);

      if (insertPoint >= 0) {
        // Add /redirect as the second component of the path so that the RM web
        // proxy knows that this request was a redirect.
        insertPoint += PROXY_PATH.length();
        redirect.insert(insertPoint, "/redirect");
      }

      ProxyUtils.sendRedirect(httpReq, httpResp, redirect.toString());
    } else {
      String user = null;

      if (httpReq.getCookies() != null) {
        for(Cookie c: httpReq.getCookies()) {
          if(WebAppProxyServlet.PROXY_USER_COOKIE_NAME.equals(c.getName())){
            user = c.getValue();
            break;
          }
        }
      }
      if (user == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not find "
              + WebAppProxyServlet.PROXY_USER_COOKIE_NAME
              + " cookie, so user will not be set");
        }

        chain.doFilter(req, resp);
      } else {
        AmIpPrincipal principal = new AmIpPrincipal(user);
        ServletRequest requestWrapper = new AmIpServletRequestWrapper(httpReq,
            principal);

        chain.doFilter(requestWrapper, resp);
      }
    }
  }

  protected String findRedirectUrl() throws ServletException {
    String addr;
    YarnConfiguration conf = new YarnConfiguration();
    String addressPropertyPrefix = YarnConfiguration.useHttps(conf)
        ? YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS
        : YarnConfiguration.RM_WEBAPP_ADDRESS;

    if ( HAUtil.isCustomRMHAEnabled(conf)) {
      // http(s)://host:port
      String currentRMAddress = WebAppUtils.getResolvedRMWebAppURLWithScheme(conf);
      String applicationWebProxy = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
        if(applicationWebProxy == null || applicationWebProxy.isEmpty()){
            for (Map.Entry<String,String> entry: proxyUriBases.entrySet()) {
                String address = entry.getValue();
                if(address.contains("/proxy/application")){
                    applicationWebProxy = address.substring(address.indexOf("/proxy/application"),address.length());
                    break;
                }
            }
            if(applicationWebProxy == null){
                applicationWebProxy = "";
            }
        }
      addr = currentRMAddress.concat(applicationWebProxy);
      try {
        URL url = new URL(currentRMAddress);
        proxyUriBases.put(url.getHost() + ":" + url.getPort(), addr);
        proxyHosts.add(url.getHost());
      } catch (MalformedURLException e) {
        LOG.warn("{} does not appear to be a valid URL", currentRMAddress, e);
      }
    } else if (proxyUriBases.size() == 1) {  // external proxy or not RM HA
      addr = proxyUriBases.values().iterator().next();
    } else {                          // RM HA
      String activeRMId = RMHAUtils.findActiveRMHAId(conf);
      String host = conf.get(
          HAUtil.addSuffix(addressPropertyPrefix, activeRMId));
      addr = proxyUriBases.get(host);
    }
    if (addr == null) {
      throw new ServletException(
          "Could not determine the proxy server for redirection");
    }
    return addr;
  }
}
