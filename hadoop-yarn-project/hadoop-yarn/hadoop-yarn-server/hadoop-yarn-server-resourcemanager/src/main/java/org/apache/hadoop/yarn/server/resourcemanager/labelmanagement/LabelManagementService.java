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
package org.apache.hadoop.yarn.server.resourcemanager.labelmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

/**
 * Wrapper Service Class on top of LabelManager
 * to mitigate issues with Unit Tests where Service can be created
 * started and stopped multiple times per run
 *
 */
public class LabelManagementService extends AbstractService {

  private LabelManager labelManager;
  
  public LabelManagementService() {
    super(LabelManagementService.class.getName());
    labelManager = LabelManager.getInstance();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    labelManager.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {  
    labelManager.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    labelManager.serviceStop();
  }
}
