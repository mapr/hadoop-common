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
package org.apache.hadoop.mapred;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

/**
 * Shell utility commands used by MapR during Task JVM creation.
 */
public class MapRShellUtil {
  /** a Unix command to get ulimit of a process. */
  public static final String ULIMIT_COMMAND = "ulimit";

  private static String getCpuSet ()
  {
    Runtime rt = Runtime.getRuntime();

    int cpust = -1, cpuend = -1;

    int numProcessors = rt.availableProcessors();
    if (numProcessors > 8) {
      cpust = 2; // reserve first 2 for infra processes
      cpuend = numProcessors-1;
    } else if (numProcessors > 4) {
      cpust = 1;
      cpuend = numProcessors-1;
    } else {
      // lesser than 4, dont assign a taskset ..
      return null;
    }

    String cpus = cpust + "-" + cpuend;
    return cpus;
  }

  /**
   * Get the Unix command for setting the maximum virtual memory available
   * to a given child process. This is only relevant when we are forking a
   * process from within the Mapper or the Reducer implementations.
   * Also see Hadoop Pipes and Hadoop Streaming.
   *
   * It also checks to ensure that we are running on a *nix platform else
   * (e.g. in Cygwin/Windows) it returns <code>null</code>.
   * @param memoryLimit virtual memory limit
   * @return a <code>String[]</code> with the ulimit command arguments or
   *         <code>null</code> if we are running on a non *nix platform or
   *         if the limit is unspecified.
   */
  public static String[] getUlimitMemoryCommand(long memoryLimit) {
    // ulimit isn't supported on Windows
    if (Shell.WINDOWS) {
      return null;
    }

    return new String[] {ULIMIT_COMMAND, "-v", String.valueOf(memoryLimit)};
  }

  /**
   * Unix commands to setup nice values, taskset etc..
   * @param conf
   * @return
   */
  public static void getSetupCmds (Configuration conf,
                                   Map<String, String> cmds) {
    if (Shell.WINDOWS) {
      return;
    }

    // we only allow lowering priority..
    int renice = conf.getInt("mapred.child.renice", -1);
    if (renice >= 0) {
      cmds.put ("renice", "renice -n " + renice + " -p $$ 1>/dev/null");
    }

    // not documented (very linux specific).
    boolean taskset = conf.getBoolean("mapred.child.taskset", false);
    if (taskset) {
      String cpuset = getCpuSet();
      if (cpuset != null) {
        cmds.put ("taskset", "taskset -p -c " + cpuset + " $$");
      }
    }

    // not documented (very linux specific). We only allow increasing the adjustment.
    int oomAdj = conf.getInt("mapred.child.oom_adj", -1);
    if (oomAdj >= 0) {
      File f = new File ("/proc/self/oom_score_adj");
      if (f.exists()) {
        // linux 2.36+ kernel
        cmds.put ("echo", "echo " + oomAdj + " > /proc/self/oom_score_adj");
      } else {
        cmds.put ("echo", "echo " + oomAdj + " > /proc/self/oom_adj");
      }
    }

    return;
  }
}
