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

import java.util.HashMap;
import java.util.Map;

public class HadoopCoreAuthenticationFilterInitializer extends FilterInitializer {

  private static final String HTTP_AUTH_PREFIX = "hadoop.http.authentication.";

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterConfig = new HashMap<>();

    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(HTTP_AUTH_PREFIX)) {
        String value = conf.get(name);
        name = name.substring(HTTP_AUTH_PREFIX.length());
        filterConfig.put(name, value);
      }
    }
    
    return filterConfig;
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("HadoopCoreAuthentication",
        HadoopCoreAuthenticationFilter.class.getName(), filterConfig);
  }
}
