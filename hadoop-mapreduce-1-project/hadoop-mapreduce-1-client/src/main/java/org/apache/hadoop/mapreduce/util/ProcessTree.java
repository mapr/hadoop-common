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

package org.apache.hadoop.mapreduce.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/** 
 * Process tree related operations
 */
public class ProcessTree {

  private static final Log LOG = LogFactory.getLog(ProcessTree.class);

  /**
   * The constants for the signals.
   */
  public static enum Signal {
    QUIT(3), KILL(9), TERM(15);
    private int value;
    private Signal(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  }

  public static final boolean isSetsidAvailable = isSetsidSupported();
  private static boolean isSetsidSupported() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
    }
    return setsidSupported;
  }

  /**
   * Send a specified signal to the specified pid
   *
   * @param pid the pid of the process [group] to signal.
   * @param signalNum the signal to send.
   * @param signalName the human-readable description of the signal
   * (for logging).
   */
  private static void sendSignal(String pid, Signal signal) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-" + signal.getValue(), pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + ioe);
    } finally {
      if (pid.startsWith("-")) {
        LOG.info("Sending signal to all members of process group " + pid
            + ": " + signal + ". Exit code " + shexec.getExitCode());
      } else {
        LOG.info("Signaling process " + pid
            + " with " + signal + ". Exit code " + shexec.getExitCode());
      }
    }
  }

  /**
   * Send a specified signal to the process, if it is alive.
   *
   * @param pid the pid of the process to signal.
   * @param signalNum the signal to send.
   * @param signalName the human-readable description of the signal
   * (for logging).
   * @param alwaysSignal if true then send signal even if isAlive(pid) is false
   */
  private static void maybeSignalProcess(String pid, Signal signal, 
      boolean alwaysSignal) {
    // If process tree is not alive then don't signal, unless alwaysSignal
    // forces it so.
    if (alwaysSignal || ProcessTree.isAlive(pid)) {
      sendSignal(pid, signal);
    }
  }

  private static void maybeSignalProcessGroup(String pgrpId, Signal signal,
      boolean alwaysSignal) {

    if (alwaysSignal || ProcessTree.isProcessGroupAlive(pgrpId)) {
      // signaling a process group means using a negative pid.
      sendSignal("-" + pgrpId, signal);
    }
  }

  /**
   * Sends terminate signal to the process, allowing it to gracefully exit.
   * 
   * @param pid pid of the process to be sent SIGTERM
   */
  public static void terminateProcess(String pid, Signal signal) {
    maybeSignalProcess(pid, signal, true);
  }

  /**
   * Sends terminate signal to all the process belonging to the passed process
   * group, allowing the group to gracefully exit.
   * 
   * @param pgrpId process group id
   */
  public static void terminateProcessGroup(String pgrpId, Signal signal) {
    maybeSignalProcessGroup(pgrpId, signal, true);
  }

  /**
   * Kills the process(OR process group) by sending the signal SIGKILL
   * in the current thread
   * @param pid Process id(OR process group id) of to-be-deleted-process
   * @param isProcessGroup Is pid a process group id of to-be-deleted-processes
   * @param sleepTimeBeforeSigKill wait time before sending SIGKILL after
   *  sending SIGTERM
   */
  private static void sigKillInCurrentThread(String pid, boolean isProcessGroup,
      long sleepTimeBeforeSigKill) {
    // Kill the subprocesses of root process(even if the root process is not
    // alive) if process group is to be killed.
    if (isProcessGroup || ProcessTree.isAlive(pid)) {
      try {
        // Sleep for some time before sending SIGKILL
        Thread.sleep(sleepTimeBeforeSigKill);
      } catch (InterruptedException i) {
        LOG.warn("Thread sleep is interrupted.");
      }
      if(isProcessGroup) {
        killProcessGroup(pid, Signal.KILL);
      } else {
        killProcess(pid, Signal.KILL);
      }
    }  
  }

  /**
   * Sends kill signal to process, forcefully terminating the process.
   * 
   * @param pid process id
   */
  public static void killProcess(String pid, Signal signal) {
    maybeSignalProcess(pid, signal, false);
  }

  /**
   * Sends kill signal to all process belonging to same process group,
   * forcefully terminating the process group.
   * 
   * @param pgrpId process group id
   */
  public static void killProcessGroup(String pgrpId, Signal signal) {
    maybeSignalProcessGroup(pgrpId, signal, false);
  }

  /**
   * Is the process with PID pid still alive?
   * This method assumes that isAlive is called on a pid that was alive not
   * too long ago, and hence assumes no chance of pid-wrapping-around.
   * 
   * @param pid pid of the process to check.
   * @return true if process is alive.
   */
  public static boolean isAlive(String pid) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (ExitCodeException ee) {
      return false;
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + shexec + ioe);
      return false;
    }
    return (shexec.getExitCode() == 0 ? true : false);
  }
  
  /**
   * Is the process group with  still alive?
   * 
   * This method assumes that isAlive is called on a pid that was alive not
   * too long ago, and hence assumes no chance of pid-wrapping-around.
   * 
   * @param pgrpId process group id
   * @return true if any of process in group is alive.
   */
  public static boolean isProcessGroupAlive(String pgrpId) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", "-"+pgrpId };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (ExitCodeException ee) {
      return false;
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + shexec + ioe);
      return false;
    }
    return (shexec.getExitCode() == 0 ? true : false);
  }
  
  /**
   * Helper thread class that kills process-tree with SIGKILL in background
   */
  static class SigKillThread extends Thread {
    private String pid = null;
    private boolean isProcessGroup = false;

    private long sleepTimeBeforeSigKill;

    private SigKillThread(String pid, boolean isProcessGroup, long interval) {
      this.pid = pid;
      this.isProcessGroup = isProcessGroup;
      this.setName(this.getClass().getName() + "-" + pid);
      sleepTimeBeforeSigKill = interval;
    }

    public void run() {
      sigKillInCurrentThread(pid, isProcessGroup, sleepTimeBeforeSigKill);
    }
  }
}
