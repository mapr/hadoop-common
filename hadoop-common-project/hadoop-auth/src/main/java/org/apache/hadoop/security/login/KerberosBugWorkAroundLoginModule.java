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
      if (ticketCache != null) {
    Map<String, Object> newOptions = new HashMap<String, Object>();
    newOptions.putAll(options);
    newOptions.put("ticketCache", ticketCache);
    options = newOptions;
     }
    super.initialize(subject, ch, sharedState, options);
  }
}
