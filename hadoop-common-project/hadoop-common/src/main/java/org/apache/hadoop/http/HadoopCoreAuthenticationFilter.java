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
