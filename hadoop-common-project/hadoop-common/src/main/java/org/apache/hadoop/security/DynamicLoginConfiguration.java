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

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

public class DynamicLoginConfiguration extends Configuration {
  private final Configuration baseConfig;
  private final Map<String, ?> overrideOptions;

  public class OverrideAppConfigurationEntry extends AppConfigurationEntry {

    public OverrideAppConfigurationEntry(AppConfigurationEntry base) {
      super(base.getLoginModuleName(), base.getControlFlag(), base.getOptions());
    }

    @Override
    public Map<String, ?> getOptions() {
      Map<String, ?> baseOptions = super.getOptions();
      Map<String, Object> newOption = new HashMap<String, Object>();
      newOption.putAll(baseOptions);
      newOption.putAll(overrideOptions);
      return newOption;
    }
  }

  public DynamicLoginConfiguration(Configuration base,  Map<String,?> options) {
    this.baseConfig = base;
    this.overrideOptions = options;
  }

  /**
   * Only goal here is to override the values of options at runtime.
   *
   * @param appName
   * @return
   */
  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    AppConfigurationEntry[] app = baseConfig.getAppConfigurationEntry(appName);
    if (app == null) {
      return null;
    }

    AppConfigurationEntry[] newEntries = new AppConfigurationEntry[app.length];
    for (int i = 0; i < app.length; i++ ) {
      AppConfigurationEntry entry = app[i];
      newEntries[i] = new OverrideAppConfigurationEntry(entry);
    }
    return newEntries;
  }
}
