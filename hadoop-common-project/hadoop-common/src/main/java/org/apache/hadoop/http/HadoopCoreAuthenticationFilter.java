package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import java.util.Map;
import java.util.Properties;

/**
 * {@link AuthenticationFilter} exposes several configuration params such as
 * {@link AuthenticationFilter#AUTH_TOKEN_VALIDITY}. Normally these params go into web.xml.
 * The problem with above is that each webserver (JT/TT/HBase) has to configure these params
 * separately in the corresponding web.xml files.
 *
 * This class overrides the above behavior to get the config params from core-site.xml.
 * All the webservers that depend on hadoop core automatically get a single config defined in core-site.xml.
 *
 * The config in core-site.xml should go like:
 *  <property>
 *    <name>hadoop.http.authentication.signature.secret</name>
 *    <value>13048203948239</value>
 *  </property>
 *  <property>
 *    <name>hadoop.http.authentication.token.validity</name>
 *    <value>48</value>
 *  </property>
 */
public class HadoopCoreAuthenticationFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
      Configuration conf = new Configuration();
      Properties props = new Properties();
      for (Map.Entry<String, String> entry : conf) {
        String name = entry.getKey();
        if (name.startsWith(configPrefix)) {
          String value = conf.get(name);
          name = name.substring(configPrefix.length());
          props.setProperty(name, value);
        }
      }
      props.putAll(super.getConfiguration(configPrefix, filterConfig));
      return props;
    }

}
