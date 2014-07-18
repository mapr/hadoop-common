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
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

@Private
@Unstable
public class DiskBasedResourceCalculator extends DefaultResourceCalculator {

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator), numerator.getVirtualCores(),
          numerator.getDisks()); //Only memory is divided
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
        roundUp(
            Math.max(r.getMemory(), minimumResource.getMemory()),
            stepFactor.getMemory()),
            maximumResource.getMemory());
    return Resources.createResource(normalizedMemory, r.getVirtualCores(), r.getDisks());
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory()), r.getVirtualCores(), r.getDisks()
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()), r.getVirtualCores(), r.getDisks());
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp((int)(r.getMemory() * by + 0.5), stepFactor.getMemory()), r.getVirtualCores(), r.getDisks()
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            ), r.getVirtualCores(), r.getDisks()
        );
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    int availableContainers = 0;

    if (required.getMemory() > 0) {
      availableContainers = available.getMemory() / required.getMemory();
    }

    if (required.getVirtualCores() > 0) {
      int availableContainersCpu = available.getVirtualCores() / required.getVirtualCores();
      if (availableContainersCpu < availableContainers) {
        availableContainers = availableContainersCpu;
      }
    }

    if (required.getDisks() > 0) {
        double availableContainersDisk = Math.floor(available.getDisks() / required.getDisks());
        if (availableContainersDisk < availableContainers) {
          availableContainers = (int) availableContainersDisk;
        }
      }
    return availableContainers;
  }
}
