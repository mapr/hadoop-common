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

package org.apache.hadoop.security.rpcauth;

import org.apache.hadoop.security.AccessControlException;

public class FatalAccessControlException extends AccessControlException {

  private static final long serialVersionUID = -2240790088795915455L;

  public FatalAccessControlException() {
    super();
  }

  public FatalAccessControlException(String s) {
    super(s);
  }

  public FatalAccessControlException(Throwable cause) {
    super(cause);
  }

  public FatalAccessControlException(String s, Throwable cause) {
    super(s);
    initCause(cause);
  }

}
