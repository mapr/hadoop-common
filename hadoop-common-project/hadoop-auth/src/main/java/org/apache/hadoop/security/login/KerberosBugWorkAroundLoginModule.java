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

package org.apache.hadoop.security.login;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import com.sun.security.auth.module.Krb5LoginModule;

/**
 * work around Java 6 bug where the KRB5CCNAME isn't honored. This is fixed
 * in later Java 7 patch levels, but many users are running old stuff so ...
 */
@SuppressWarnings("restriction")
public class KerberosBugWorkAroundLoginModule extends Krb5LoginModule {
  @Override
  public void initialize(Subject subject, CallbackHandler ch,
      Map<String, ?> sharedState, Map<String, ?> options) {
    String ticketCache = System.getenv("KRB5CCNAME");
    if (ticketCache != null && "true".equalsIgnoreCase((String)options.get("useTicketCache"))) {
      Map<String, Object> newOptions = new HashMap<String, Object>();
      newOptions.putAll(options);
      newOptions.put("ticketCache", ticketCache);
      options = newOptions;
    }
    super.initialize(subject, ch, sharedState, options);
  }
}
