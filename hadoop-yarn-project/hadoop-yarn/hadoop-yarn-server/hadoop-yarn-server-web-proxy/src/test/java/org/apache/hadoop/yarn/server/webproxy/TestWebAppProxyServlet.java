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

package org.apache.hadoop.yarn.server.webproxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the WebAppProxyServlet and WebAppProxy. For back end use simple web
 * server.
 */
public class TestWebAppProxyServlet {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestWebAppProxyServlet.class);

  private static Server server;
  private static int originalPort = 0;
  Configuration configuration = new Configuration();

  /**
   * Simple http server. Server should send answer with status 200
   */
  @BeforeClass
  public static void start() throws Exception {
    server = new Server(0);
    Context context = new Context();
    context.setContextPath("/foo");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/bar");
    server.getConnectors()[0].setHost("localhost");
    server.start();
    originalPort = server.getConnectors()[0].getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:"
        + originalPort);
  }

  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      InputStream is = req.getInputStream();
      OutputStream os = resp.getOutputStream();
      int c = is.read();
      while (c > -1) {
        os.write(c);
        c = is.read();
      }
      is.close();
      os.close();
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @Test(timeout=5000)
  public void testWebAppProxyServlet() throws Exception {

    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS 
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest(originalPort);
    proxy.init(configuration);
    proxy.start();

    final int proxyPort = proxy.getProxyPort();
    WebAppProxyServerForTest.AppReportFetcherForTest appReportFetcher = proxy.getAppReportFetcher();

    // wrong url
    try {
      // wrong url without app ID
      URL emptyUrl = new URL("http://localhost:" + proxyPort + "/proxy");
      HttpURLConnection emptyProxyConn = (HttpURLConnection) emptyUrl
          .openConnection();
      emptyProxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND, emptyProxyConn.getResponseCode());

      // wrong url. Set wrong app ID
      URL wrongUrl = new URL("http://localhost:" + proxyPort + "/proxy/app");
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
          .openConnection();

      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());
      // set true Application ID in url
      URL url = new URL("http://localhost:" + proxyPort + "/proxy/application_00_0");
      proxyConn = (HttpURLConnection) url.openConnection();
      // set cookie
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      assertTrue(WebAppProxyServerForTest.isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // test that redirection is squashed correctly
      URL redirectUrl = new URL("http://localhost:" + proxyPort
          + "/proxy/redirect/application_00_0");
      proxyConn = (HttpURLConnection) redirectUrl.openConnection();
      proxyConn.setInstanceFollowRedirects(false);
      proxyConn.connect();
      assertEquals("The proxy returned an unexpected status code rather than"
          + "redirecting the connection (302)",
          HttpURLConnection.HTTP_MOVED_TEMP, proxyConn.getResponseCode());

      String expected =
          WebAppUtils.getResolvedRMWebAppURLWithScheme(configuration)
            + "/cluster/failure/application_00_0";
      String redirect = proxyConn.getHeaderField(ProxyUtils.LOCATION);

      assertEquals("The proxy did not redirect the connection to the failure "
          + "page of the RM", expected, redirect);

      // cannot found application 1: null
      appReportFetcher.setAnswer(1);
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          proxyConn.getResponseCode());
      assertFalse(WebAppProxyServerForTest.isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // cannot found application 2: ApplicationNotFoundException
      appReportFetcher.setAnswer(4);
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          proxyConn.getResponseCode());
      assertFalse(WebAppProxyServerForTest.isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));

      // wrong user
      appReportFetcher.setAnswer(2);
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      String s = readInputStream(proxyConn.getInputStream());
      assertTrue(s
          .contains("to continue to an Application Master web interface owned by"));
      assertTrue(s.contains("WARNING: The following page may not be safe!"));

      //case if task has a not running status
      appReportFetcher.setAnswer(3);
      proxyConn = (HttpURLConnection) url.openConnection();
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());

      // test user-provided path and query parameter can be appended to the
      // original tracking url
      appReportFetcher.setAnswer(5);
      URL clientUrl = new URL("http://localhost:" + proxyPort
        + "/proxy/application_00_0/test/tez?x=y&h=p");
      proxyConn = (HttpURLConnection) clientUrl.openConnection();
      proxyConn.connect();
      LOG.info("" + proxyConn.getURL());
      LOG.info("ProxyConn.getHeaderField(): " +  proxyConn.getHeaderField(ProxyUtils.LOCATION));
      assertEquals("http://localhost:" + originalPort
          + "/foo/bar/test/tez?a=b&x=y&h=p#main", proxyConn.getURL().toString());

    } finally {
      proxy.close();
    }
  }

  @Test(timeout=5000)
  public void testAppReportForEmptyTrackingUrl() throws Exception {
    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest(originalPort);
    proxy.init(configuration);
    proxy.start();

    final int proxyPort = proxy.getProxyPort();
    WebAppProxyServerForTest.AppReportFetcherForTest appReportFetcher = proxy.getAppReportFetcher();

    try {
    //set AHS_ENBALED = false to simulate getting the app report from RM
    configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        false);
    ApplicationId app = ApplicationId.newInstance(0, 0);
    appReportFetcher.setAnswer(6);
    URL url = new URL("http://localhost:" + proxyPort +
        "/proxy/" + app.toString());
    HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
    proxyConn.connect();
    try {
      proxyConn.getResponseCode();
    } catch (ConnectException e) {
      // Connection Exception is expected as we have set
      // appReportFetcher.answer = 6, which does not set anything for
      // original tracking url field in the app report.
    }
    String appAddressInRm =
        WebAppUtils.getResolvedRMWebAppURLWithScheme(configuration) +
        "/cluster" + "/app/" + app.toString();
    assertTrue("Webapp proxy servlet should have redirected to RM",
        proxyConn.getURL().toString().equals(appAddressInRm));

    //set AHS_ENBALED = true to simulate getting the app report from AHS
    configuration.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        true);
    proxyConn = (HttpURLConnection) url.openConnection();
    proxyConn.connect();
    try {
      proxyConn.getResponseCode();
    } catch (ConnectException e) {
      // Connection Exception is expected as we have set
      // appReportFetcher.answer = 6, which does not set anything for
      // original tracking url field in the app report.
    }
    String appAddressInAhs = WebAppUtils.getHttpSchemePrefix(configuration) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(configuration) +
        "/applicationhistory" + "/app/" + app.toString();
    assertTrue("Webapp proxy servlet should have redirected to AHS",
        proxyConn.getURL().toString().equals(appAddressInAhs));
    }
    finally {
      proxy.close();
    }
  }

  /**
   * Test main method of WebAppProxyServer
   */
  @Test(timeout=5000)
  public void testWebAppProxyServerMainMethod() throws Exception {
    WebAppProxyServer mainServer = null;
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9099");
    try {
      mainServer  = WebAppProxyServer.startServer(conf);
      int counter = 20;

      URL wrongUrl = new URL("http://localhost:9099/proxy/app");
      HttpURLConnection proxyConn = null;
      while (counter > 0) {
        counter--;
        try {
          proxyConn = (HttpURLConnection) wrongUrl.openConnection();
          proxyConn.connect();
          proxyConn.getResponseCode();
          // server started ok
          counter = 0;
        } catch (Exception e) {
          Thread.sleep(100);
        }
      }
      assertNotNull(proxyConn);
      // wrong application Id
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());
    } finally {
      if (mainServer != null) {
        mainServer.stop();
      }
    }
  }

  /**
   * Test header injections are not done.
   */
  @Test(timeout=5000)
  public void testWebAppProxyServerHeaderInjection() throws Exception {
    WebAppProxyServer mainServer = null;
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9099");
    try {
      mainServer = WebAppProxyServer.startServer(conf);
      int counter = 20;

      URL wrongUrl = new URL(
          "http://localhost:9099/proxy/%C4%8D%C4%8ASomeCustomInjectedHeader:%20"
          + "injected_headerVal_1484290871375_0113/");
      HttpURLConnection proxyConn = null;
      while (counter > 0) {
        counter--;
        try {
          proxyConn = (HttpURLConnection) wrongUrl.openConnection();
          proxyConn.connect();
          proxyConn.getResponseCode();
          // server started ok
          counter = 0;
        } catch (Exception e) {
          Thread.sleep(100);
        }
      }
      assertNotNull(proxyConn);
      // wrong application Id
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR,
          proxyConn.getResponseCode());
      assertTrue("Header injection happened",
          proxyConn.getHeaderField("SomeCustomInjectedHeader") == null);
    } finally {
      if (mainServer != null) {
        mainServer.stop();
      }
    }
  }

  private String readInputStream(InputStream input) throws Exception {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    byte[] buffer = new byte[512];
    int read;
    while ((read = input.read(buffer)) >= 0) {
      data.write(buffer, 0, read);
    }
    return new String(data.toByteArray(), "UTF-8");
  }

  @AfterClass
  public static void stop() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
    }

    try {
      server.destroy();
    } catch (Exception e) {
    }
  }

}
