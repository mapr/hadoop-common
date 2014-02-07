package org.apache.hadoop.security.login;

/**
 * Just to simplify configuration, this login module wraps the NT and Unix
 * login modules and does the right thing depending on platform.
 */

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
import static org.apache.hadoop.util.PlatformName.IS_64BIT;
import static org.apache.hadoop.util.PlatformName.IS_AIX;
import static org.apache.hadoop.util.PlatformName.IS_WINDOWS;

public class GenericOSLoginModule implements LoginModule {
  private LoginModule realModule;

  /* Return the OS login module class name */
  private static String getOSLoginModuleName() {
    if (IBM_JAVA) {
      if (IS_WINDOWS) {
        return IS_64BIT
            ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (IS_AIX) {
        return IS_64BIT
            ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return IS_WINDOWS
          ? "com.sun.security.auth.module.NTLoginModule"
          : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  public static final String OS_LOGIN_MODULE_NAME;
  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
  }

  @Override
  public boolean abort() throws LoginException {
    return realModule.abort();
  }

  @Override
  public boolean commit() throws LoginException {
    return realModule.commit();
  }

  @Override
  public void initialize(Subject arg0, CallbackHandler arg1,
      Map<String, ?> arg2, Map<String, ?> arg3) {
    try {
      realModule = (LoginModule) Class.forName(OS_LOGIN_MODULE_NAME).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failure to instantiate needed login module: "
                                 + OS_LOGIN_MODULE_NAME, e);
    }

    realModule.initialize(arg0,  arg1, arg2, arg3);
  }

  @Override
  public boolean login() throws LoginException {
    return realModule.login();
  }

  @Override
  public boolean logout() throws LoginException {
    return realModule.logout();
  }
}
