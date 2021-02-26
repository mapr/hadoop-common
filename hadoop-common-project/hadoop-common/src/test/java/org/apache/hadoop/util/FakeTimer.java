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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.concurrent.TimeUnit;

/**
 * FakeTimer can be used for test purposes to control the return values
 * from {{@link Timer}}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FakeTimer extends Timer {
  private long nowMillis;
  private long nowNanos;

  /** Constructs a FakeTimer with a non-zero value */
  public FakeTimer() {
    nowMillis = 1000;  // Initialize with a non-trivial value.
    nowNanos = TimeUnit.MILLISECONDS.toNanos(1000);
  }

  @Override
  public long now() {
    return nowMillis;
  }

  @Override
  public long monotonicNow() {
    return nowMillis;
  }

  @Override
  public long monotonicNowNanos() {
    return nowNanos;
  }

  /** Increases the time by milliseconds */
  public void advance(long advMillis) {
    nowMillis += advMillis;
  }
}
