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
 *    <name>hadoop.auth.signature.secret</name>
 *    <value>13048203948239</value>
 *  </property>
 *  <property>
 *    <name>hadoop.auth.token.validity</name>
 *    <value>48</value>
 *  </property>
 *
 * The default values are hardcoded in {@link AuthenticationFilter} itself.
 *
 * Author: smarella
 */
public class HadoopCoreAuthenticationFilter extends AuthenticationFilter {

    public static final String HADOOP_AUTH_PREFIX = "hadoop.auth.";

    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
      Configuration conf = new Configuration();

      Properties props = new Properties();
      for (Map.Entry<String, String> entry : conf) {
        String name = entry.getKey();
        if (name.startsWith(HADOOP_AUTH_PREFIX)) {
          String value = conf.get(name);
          name = name.substring(HADOOP_AUTH_PREFIX.length());
          props.setProperty(name, value);
        }
      }
      props.putAll(super.getConfiguration(configPrefix, filterConfig));
      return props;
    }

}
