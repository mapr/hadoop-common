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

import com.google.common.base.Supplier;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.util.Shell.*;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assume;
import org.junit.Test;

public class TestShell extends TestCase {

  private static class Command extends Shell {
    private int runCount = 0;

    private Command(long interval) {
      super(interval);
    }

    @Override
    protected String[] getExecString() {
      // There is no /bin/echo equivalent on Windows so just launch it as a
      // shell built-in.
      //
      return Shell.WINDOWS ?
          (new String[] {"cmd.exe", "/c", "echo", "hello"}) :
          (new String[] {"echo", "hello"});
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      ++runCount;
    }

    public int getRunCount() {
      return runCount;
    }
  }

  public void testInterval() throws IOException {
    testInterval(Long.MIN_VALUE / 60000);  // test a negative interval
    testInterval(0L);  // test a zero interval
    testInterval(10L); // interval equal to 10mins
    testInterval(Time.now() / 60000 + 60); // test a very big interval
  }

  /**
   * Assert that a string has a substring in it
   * @param string string to search
   * @param search what to search for it
   */
  private void assertInString(String string, String search) {
    assertNotNull("Empty String", string);
    if (!string.contains(search)) {
      fail("Did not find \"" + search + "\" in " + string);
    }
  }

  public void testShellCommandExecutorToString() throws Throwable {
    Shell.ShellCommandExecutor sce=new Shell.ShellCommandExecutor(
            new String[] { "ls", "..","arg 2"});
    String command = sce.toString();
    assertInString(command,"ls");
    assertInString(command, " .. ");
    assertInString(command, "\"arg 2\"");
  }
  
  public void testShellCommandTimeout() throws Throwable {
    if(Shell.WINDOWS) {
      // setExecutable does not work on Windows
      return;
    }
    String rootDir = new File(System.getProperty(
        "test.build.data", "/tmp")).getAbsolutePath();
    File shellFile = new File(rootDir, "timeout.sh");
    String timeoutCommand = "sleep 4; echo \"hello\"";
    PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
    writer.println(timeoutCommand);
    writer.close();
    FileUtil.setExecutable(shellFile, true);
    Shell.ShellCommandExecutor shexc 
    = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()},
                                      null, null, 100);
    try {
      shexc.execute();
    } catch (Exception e) {
      //When timing out exception is thrown.
    }
    shellFile.delete();
    assertTrue("Script didnt not timeout" , shexc.isTimedOut());
  }

  @Test
  public void testEnvVarsWithInheritance() throws Exception {
    Assume.assumeFalse(WINDOWS);
    testEnvHelper(true);
  }

  @Test
  public void testEnvVarsWithoutInheritance() throws Exception {
    Assume.assumeFalse(WINDOWS);
    testEnvHelper(false);
  }

  private void testEnvHelper(boolean inheritParentEnv) throws Exception {
    Map<String, String> customEnv = Collections.singletonMap(
            JavaKeyStoreProvider.CREDENTIAL_PASSWORD_NAME, "foo");
    Shell.ShellCommandExecutor command = new ShellCommandExecutor(
            new String[]{"env"}, null, customEnv, 0L,
            inheritParentEnv);
    command.execute();
    String[] varsArr = command.getOutput().split("\n");
    Map<String, String> vars = new HashMap<>();
    for (String var : varsArr) {
      int eqIndex = var.indexOf('=');
      vars.put(var.substring(0, eqIndex), var.substring(eqIndex + 1));
    }
    Map<String, String> expectedEnv = new HashMap<>();
    expectedEnv.putAll(System.getenv());
    if (inheritParentEnv) {
      expectedEnv.putAll(customEnv);
    } else {
      assertFalse("child process environment should not have contained "
                      + JavaKeyStoreProvider.CREDENTIAL_PASSWORD_NAME,
              vars.containsKey(
                      JavaKeyStoreProvider.CREDENTIAL_PASSWORD_NAME));
    }
    assertEquals(expectedEnv, vars);
  }
  
  private static int countTimerThreads() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    
    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains("Timer")) {
          count++;
          break;
        }
      }
    }
    return count;
  }
  
  public void testShellCommandTimerLeak() throws Exception {
    String quickCommand[] = new String[] {"/bin/sleep", "100"};
    
    int timersBefore = countTimerThreads();
    System.err.println("before: " + timersBefore);
    
    for (int i = 0; i < 10; i++) {
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
            quickCommand, null, null, 1);
      try {
        shexec.execute();
        fail("Bad command should throw exception");
      } catch (Exception e) {
        // expected
      }
    }
    Thread.sleep(1000);
    int timersAfter = countTimerThreads();
    System.err.println("after: " + timersAfter);
    assertEquals(timersBefore, timersAfter);
  }
  

  private void testInterval(long interval) throws IOException {
    Command command = new Command(interval);

    command.run();
    assertEquals(1, command.getRunCount());

    command.run();
    if (interval > 0) {
      assertEquals(1, command.getRunCount());
    } else {
      assertEquals(2, command.getRunCount());
    }
  }

  @Test(timeout=120000)
  public void testShellKillAllProcesses() throws Throwable {
    Assume.assumeFalse(WINDOWS);
    StringBuffer sleepCommand = new StringBuffer();
    sleepCommand.append("sleep 200");
    String[] shellCmd = {"bash", "-c", sleepCommand.toString()};
    final ShellCommandExecutor shexc1 = new ShellCommandExecutor(shellCmd);
    final ShellCommandExecutor shexc2 = new ShellCommandExecutor(shellCmd);

    Thread shellThread1 = new Thread() {
      @Override
      public void run() {
        try {
          shexc1.execute();
        } catch(IOException ioe) {
          //ignore IOException from thread interrupt
        }
      }
    };
    Thread shellThread2 = new Thread() {
      @Override
      public void run() {
        try {
          shexc2.execute();
        } catch(IOException ioe) {
          //ignore IOException from thread interrupt
        }
      }
    };

    shellThread1.start();
    shellThread2.start();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return shexc1.getProcess() != null;
      }
    }, 10, 10000);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return shexc2.getProcess() != null;
      }
    }, 10, 10000);

    Shell.destroyAllProcesses();
    shexc1.getProcess().waitFor();
    shexc2.getProcess().waitFor();
  }
}
