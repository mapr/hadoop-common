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

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
import static org.apache.hadoop.util.PlatformName.IS_64BIT;
import static org.apache.hadoop.util.PlatformName.IS_AIX;
import static org.apache.hadoop.util.PlatformName.IS_WINDOWS;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.User;

/**
 * A login module that looks at the Kerberos, Unix, or Windows principal and
 * adds the corresponding UserName.
 */
@InterfaceAudience.Private
public class HadoopLoginModule implements LoginModule {
  private static final Log LOG = LogFactory.getLog(HadoopLoginModule.class);

  private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;

  /* Return the OS principal class */
  @SuppressWarnings("unchecked")
  private static Class<? extends Principal> getOsPrincipalClass() {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try {
      String principalClass = null;
      if (IBM_JAVA) {
        if (IS_64BIT) {
          principalClass = "com.ibm.security.auth.UsernamePrincipal";
        } else {
          if (IS_WINDOWS) {
            principalClass = "com.ibm.security.auth.NTUserPrincipal";
          } else if (IS_AIX) {
            principalClass = "com.ibm.security.auth.AIXPrincipal";
          } else {
            principalClass = "com.ibm.security.auth.LinuxPrincipal";
          }
        }
      } else {
        principalClass = IS_WINDOWS
            ? "com.sun.security.auth.NTUserPrincipal"
            : "com.sun.security.auth.UnixPrincipal";
      }
      return (Class<? extends Principal>) cl.loadClass(principalClass);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find JAAS classes:" + e.getMessage());
    }
    return null;
  }
  static {
    OS_PRINCIPAL_CLASS = getOsPrincipalClass();
  }

  List<Class<? extends Principal>> principalPriority =
      new ArrayList<Class<? extends Principal>>();
  List<Class<? extends Principal>> additionalPrincipals =
      new ArrayList<Class<? extends Principal>>();

  private Subject subject;

  @Override
  public boolean abort() throws LoginException {
    return true;
  }

  private <T extends Principal> T getCanonicalUser(Class<T> cls) {
    for(T user: subject.getPrincipals(cls)) {
      if (cls.isInstance(user)) {
        return user;
      }
    }
    return null;
  }

  /** 
   * In general the User object is used for identity, but in some cases
   * principals may be accessed directly. This code also checks to see if the
   * IDs are "the same." If they are not, that is almost certainly a serious
   * problem and we issue an error.
   */
  private void warnIfIdentityAmbiguity(User user, Principal p) {
    User tuser = new User(p.getName());
    if (! user.getShortName().equals(tuser.getShortName())) {
      LOG.error("Possible identity ambiguity. Found these two different names"
          + " in Subject: " + user + ", " + tuser + ". Two different identities"
          + " were found by the LoginModules in the JAAS configuration file.");
    }
  }

  /**
   * Try to find a user in the subject to be the Hadoop user. The options define
   * the preferred ordering of principals that should be used. The one's
   * specified go after Kerberos but before the OS Hadoop defaults since the OS
   * id is rarely useful. If the first principalPriority element is 'clear' then
   * all hadoop defaults are discarded.
   */
  @Override
  public boolean commit() throws LoginException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("hadoop login commit");
    }
    // if we already have a user, we are done.
    if (!subject.getPrincipals(User.class).isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using existing subject:" + subject.getPrincipals());
      }
      return true;
    }
    User user = null;

    for (Class<? extends Principal> possiblePrincipal : principalPriority) {
      Principal p = getCanonicalUser(possiblePrincipal);
      if (p != null) {
        if (user == null) {
          user = new User(p.getName());
        } else {
          warnIfIdentityAmbiguity(user, p);
        }
      }
    }

    //try backup principals (OS)
    if (user == null) {
      for (Class<? extends Principal> possiblePrincipal : additionalPrincipals) {
        Principal p = getCanonicalUser(possiblePrincipal);
        if (p != null) {
          user = new User(p.getName());
          break;
        }
      }
    }

    // if we found the preferred user, add Hadoop principal
    if (user != null) {
      subject.getPrincipals().add(user);
      return true;
    }
    LOG.error("Can't find expected Hadoop user in " + subject);
    throw new LoginException("Can't find user name");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    this.subject = subject;

    try {
      boolean clearSpecified = false;

      String principalString = (String) options.get("principalPriority");
      if (principalString != null) {
        principalString = principalString.trim();
        String classes[] = principalString.split(",[\\s]*");

        int start;
        if (classes[0].equalsIgnoreCase("clear")) {
          clearSpecified = true;
          start = 1;
        } else {
          start = 0;
        }
        for (int i = start; i < classes.length; i++) {
          principalPriority.add((Class<? extends Principal>) Class.forName(classes[i]));
        }
      }
       if (!clearSpecified) {
        //Kerberos is first unless clear specified
        principalPriority.add(0, KerberosPrincipal.class);
      }

      if (!clearSpecified) {
         //always add the OS ones as last ditch unless cleared
        additionalPrincipals.add(OS_PRINCIPAL_CLASS);
      }
      if (LOG.isDebugEnabled()) {
        String list = Arrays.toString(principalPriority.toArray());
        LOG.debug("Priority principal search list is " + list);
        list = Arrays.toString(additionalPrincipals.toArray());
        LOG.debug("Additional principal search list is " + list);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure to initialize Hadoop login module", e);
    }
  }

  @Override
  public boolean login() throws LoginException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("hadoop login");
    }
    return true;
  }

  @Override
  public boolean logout() throws LoginException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("hadoop logout");
    }
    return true;
  }
}

