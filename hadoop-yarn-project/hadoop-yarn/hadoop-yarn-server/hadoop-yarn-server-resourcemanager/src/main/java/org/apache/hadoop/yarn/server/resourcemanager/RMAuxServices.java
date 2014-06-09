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
package org.apache.hadoop.yarn.server.resourcemanager;

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
import org.apache.hadoop.yarn.server.api.RMAuxiliaryService;

import com.google.common.base.Preconditions;

/**
 * Manages a set of @link{RMAuxiliaryService} instances by starting and
 * stopping them.
 *
 * Note: The code has been taken from @link{AuxServices}. This could be
 * refactored into base, derived class where the derived class provides the
 * property names such as aux services, format, etc.
 */
public class RMAuxServices extends AbstractService
  implements ServiceStateChangeListener {

  private static final Log LOG = LogFactory.getLog(RMAuxServices.class);

  private final Map<String, RMAuxiliaryService> serviceMap;

  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  public RMAuxServices() {
    super(RMAuxServices.class.getName());

    serviceMap =
      Collections.synchronizedMap(new HashMap<String, RMAuxiliaryService>());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Collection<String> auxNames = conf.getStringCollection(
        YarnConfiguration.RM_AUX_SERVICES);
    for (final String sName : auxNames) {
      try {
        Preconditions
            .checkArgument(
                validateAuxServiceName(sName),
                "The ServiceName: " + sName + " set in " +
                YarnConfiguration.RM_AUX_SERVICES + " is invalid." +
                "The valid service name should only contain a-zA-Z0-9_ " +
                "and can not start with numbers");

        Class<? extends RMAuxiliaryService> sClass = conf.getClass(
              String.format(YarnConfiguration.RM_AUX_SERVICE_FMT, sName), null,
              RMAuxiliaryService.class);

        if (null == sClass) {
          throw new RuntimeException("No class defined for " + sName);
        }

        RMAuxiliaryService s = ReflectionUtils.newInstance(sClass, conf);
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
    for (Map.Entry<String, RMAuxiliaryService> entry : serviceMap.entrySet()) {
      RMAuxiliaryService service = entry.getValue();
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
      RMAuxiliaryService service) {

    if (LOG.isInfoEnabled()) {
      LOG.info("Adding RM auxiliary service " +
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
