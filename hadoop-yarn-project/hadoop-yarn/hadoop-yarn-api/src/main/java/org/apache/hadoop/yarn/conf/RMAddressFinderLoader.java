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
package org.apache.hadoop.yarn.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Class to load custom RMAddressFinder
 * ideally all the loading should be done in the constructor
 * but there are issues with static initialization ordering 
 * that dictated loading this class over "get" method and 
 * make it synchronized to prevent from double loading
 */
public class RMAddressFinderLoader {

  private static RMAddressFinderLoader s_instance = new RMAddressFinderLoader();

  private static Log LOG = LogFactory.getLog(RMAddressFinderLoader.class);
    
  private CustomRMAddressFinder rmAddressFinder;
  private volatile boolean isLoaded;
  
  private RMAddressFinderLoader() {
  }
  
  public static RMAddressFinderLoader getInstance() {
    return s_instance;
  }
  
  /**
   * getCustomRMAddressFinder method returns instance of CustomRMAddressFinder class
   * since we just need a single instance of that class it will load class if first time
   * and will create instance of the class, otherwise it will just return existing instance
   * @param conf
   * @return CustomRMAddressFinder - can be null
   */
  public synchronized CustomRMAddressFinder getCustomRMAddressFinder(Configuration conf) {
    if ( isLoaded ) {
      return rmAddressFinder;
    } 
    String className = YarnConfiguration.CUSTOM_RM_HA_RMFINDER;
    
    try {
      Class<? extends CustomRMAddressFinder> claz = conf.getClass(className, null, CustomRMAddressFinder.class);
      if ( claz != null ) {
        rmAddressFinder = claz.newInstance();
      } else {
        LOG.warn("Did not find configuration for: " + className);
      }
    } catch (InstantiationException e) {
      LOG.error("Class InstantiationException", e);
    } catch (IllegalAccessException e) {
      LOG.error("IllegalAccessException", e);
    } catch (Throwable t) {
      LOG.error("Exception while trying to instantiate class with name under property: " + className, t); 
    }
    isLoaded = true;
    return rmAddressFinder;
  }
}
