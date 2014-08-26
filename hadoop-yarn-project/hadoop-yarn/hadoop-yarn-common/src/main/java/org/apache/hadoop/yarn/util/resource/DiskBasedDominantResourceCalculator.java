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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;

public class DiskBasedDominantResourceCalculator extends
  DominantResourceCalculator {

  private static final Log LOG = LogFactory.getLog(DiskBasedDominantResourceCalculator.class);
  
  private static final double ZERO_LIMIT = 0.0099999999999999;

  /*
   * Anything greater than -0.01 and less than 0.01 is zero
   */
  private static boolean isZero(double a) {
    if(Math.abs(a) > ZERO_LIMIT) {
      return false;
    }
    return true;
  }

  public static double divideAndCeilDouble(double a, double b) {
    if (isZero(b)) {
      LOG.info("divideAndCeilDouble called with a=" + a + " b=" + b);
      return 0;
    }
    return Math.ceil(a/b);
  }

  /*
   * Special method for rounding up disk. Since the default minimum allocation is <mem:1024, cpu:1, disk:0.0>
   * the argument b can be zero if the disk is 0.0. In such a case we don't do any rounding and
   * return argument a back.
   */
  public static double roundUpDisk(double a, double b) {
    return (isZero(b))? a: divideAndCeilDouble(a, b) * b;
  }

  public static double divideAndFloorDouble(double a, double b) {
    if (isZero(b)) {
      LOG.info("divideAndFloorDouble called with a=" + a + " b=" + b);
      return 0;
    }
    return Math.floor(a/b);
  }

  /*
   * Special method for rounding down disk. Since the default minimum allocation is <mem:1024, cpu:1, disk:0.0>
   * the argument b can be zero if the disk is 0.0. In such a case we don't do any rounding and
   * return argument a back.
   */
  public static double roundDownDisk(double a, double b) {
    return (isZero(b))? a: divideAndFloorDouble(a, b) * b;
  }

  /**
   * Use 'dominant' for now since we only have 2 resources - gives us a slight
   * performance boost.
   * 
   * Once we add more resources, we'll need a more complicated (and slightly
   * less performant algorithm).
   */
  @Override
  protected float getResourceAsValue(
      Resource clusterResource, Resource resource, boolean dominant) {
    // Just use 'dominant' resource
    return (dominant) ?
        Math.max(
          Math.max(
            (float)resource.getMemory() / clusterResource.getMemory(),
            (float)resource.getVirtualCores() / clusterResource.getVirtualCores()
            ),
          (float)(resource.getDisks() / clusterResource.getDisks()) //disks in the entire cluster cannot be zero
        )
        :
        Math.min(
          Math.min(
            (float)resource.getMemory() / clusterResource.getMemory(),
            (float)resource.getVirtualCores() / clusterResource.getVirtualCores()
          ),
          (float)(resource.getDisks() / clusterResource.getDisks()) //disks in the entire cluster cannot be zero
        );
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    int min = Math.min(
      available.getMemory() / required.getMemory(),
      available.getVirtualCores() / required.getVirtualCores()
      );

    if (!isZero(required.getDisks())) { //disk can be zero
      min = Math.min(min,
        (int)(available.getDisks() / required.getDisks())
      );
    }
    return min;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    float max = Math.max(
      (float)a.getMemory()/b.getMemory(), 
      (float)a.getVirtualCores()/b.getVirtualCores()
    );

    if(!isZero(b.getDisks())) { //disk can be zero
      max = Math.max(max,
        (float)(a.getDisks()/b.getDisks())
      );
    }
    return max;
}

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
      divideAndCeil(numerator.getMemory(), denominator),
      divideAndCeil(numerator.getVirtualCores(), denominator),
      divideAndCeilDouble(numerator.getDisks(), denominator)
    );
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
      roundUp(
          Math.max(r.getMemory(), minimumResource.getMemory()),
            stepFactor.getMemory()),
        maximumResource.getMemory());
    int normalizedCores = Math.min(
      roundUp(
          Math.max(r.getVirtualCores(), minimumResource.getVirtualCores()),
            stepFactor.getVirtualCores()),
        maximumResource.getVirtualCores());
    double normalizedDisk = Math.min(
      roundUpDisk(
        Math.max(r.getDisks(), minimumResource.getDisks()),
        stepFactor.getDisks()),
      maximumResource.getDisks());
    return Resources.createResource(normalizedMemory, normalizedCores,
                  normalizedDisk);
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
      roundUp(r.getMemory(), stepFactor.getMemory()),
      roundUp(r.getVirtualCores(), stepFactor.getVirtualCores()),
      roundUpDisk(r.getDisks(), stepFactor.getDisks())
    );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
      roundDown(r.getMemory(), stepFactor.getMemory()),
      roundDown(r.getVirtualCores(), stepFactor.getVirtualCores()),
      roundDownDisk(r.getDisks(), stepFactor.getDisks())
    );
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
      roundUp(
        (int)Math.ceil(r.getMemory() * by), stepFactor.getMemory()),
      roundUp(
        (int)Math.ceil(r.getVirtualCores() * by), stepFactor.getVirtualCores()),
      roundUpDisk(
        r.getDisks() * by, stepFactor.getDisks())
    );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
          (int)(r.getMemory() * by), stepFactor.getMemory()),
        roundDown(
          (int)(r.getVirtualCores() * by), stepFactor.getVirtualCores()),
        roundDownDisk(r.getDisks() * by, stepFactor.getDisks() )
        );
  }

}
