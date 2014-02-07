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
