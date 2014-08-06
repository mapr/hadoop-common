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
package org.apache.hadoop.yarn.server.api;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Preconditions;

/**
 * Manages a set of @link{ConfigurableAuxiliaryService} instances by starting
 * and stopping them. It takes a parameter <code>auxServicesPropName</code> and
 * uses it to determine the auxiliary service classes to instantiate.
 *
 * Since this is meant to be an extensibility feature, the calling code should
 * add this instance as the first one to its service list. This will ensure
 * that the externally plugged in services are initialized first as they may
 * be setting up critical resources on top of which rest of the stack should
 * be built. For e.g., an external service might try to create volumes on top
 * of which directories need to be created.
 *
 * Note: The code has been taken from @link{AuxServices}.
 */
public class ConfigurableAuxServices extends AbstractService
  implements ServiceStateChangeListener {

  private static final Log LOG = LogFactory.getLog(ConfigurableAuxServices.class);

  private final Map<String, ConfigurableAuxiliaryService> serviceMap;

  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  private final String auxServicesPropName;

  public ConfigurableAuxServices(String name, String auxServicesPropName) {
    super(name);

    this.auxServicesPropName = auxServicesPropName;

    serviceMap =
      Collections.synchronizedMap(new HashMap<String, ConfigurableAuxiliaryService>());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Collection<String> auxNames = conf.getStringCollection(auxServicesPropName);
    for (final String sName : auxNames) {
      try {
        Preconditions
            .checkArgument(
                validateAuxServiceName(sName),
                "The ServiceName: " + sName + " set in " +
                auxServicesPropName + " is invalid." +
                "The valid service name should only contain a-zA-Z0-9_ " +
                "and can not start with numbers");

        Class<? extends ConfigurableAuxiliaryService> sClass = conf.getClass(
              String.format(YarnConfiguration.AUX_SERVICE_FMT, sName), null,
              ConfigurableAuxiliaryService.class);

        if (null == sClass) {
          throw new RuntimeException("No class defined for " + sName);
        }

        ConfigurableAuxiliaryService s = ReflectionUtils.newInstance(sClass, conf);
        addService(sName, s);
        s.init(conf);
      } catch (RuntimeException e) {
        LOG.fatal("Failed to initialize " + sName, e);
        throw e;
      }
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    for (Map.Entry<String, ConfigurableAuxiliaryService> entry : serviceMap.entrySet()) {
      ConfigurableAuxiliaryService service = entry.getValue();
      service.start();
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    try {
      synchronized (serviceMap) {
        for (Service service : serviceMap.values()) {
          if (service.getServiceState() == Service.STATE.STARTED) {
            service.stop();
          }
        }
        serviceMap.clear();
      }
    } finally {
      super.serviceStop();
    }
  }

  @Override
  public void stateChanged(Service service) {
    LOG.fatal("Service " + service.getName() + " changed state: " +
        service.getServiceState());

    stop();
  }

  private final synchronized void addService(String name,
      ConfigurableAuxiliaryService service) {

    if (LOG.isInfoEnabled()) {
      LOG.info("Adding auxiliary service " +
          service.getName() + ", \"" + name + "\"");
    }

    serviceMap.put(name, service);
  }

  private boolean validateAuxServiceName(String name) {
    if (name == null || name.trim().isEmpty()) {
      return false;
    }
    return p.matcher(name).matches();
  }
}
