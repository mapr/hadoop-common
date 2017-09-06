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
package org.apache.hadoop.yarn.sls.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.utils.JobUtils;
import org.apache.hadoop.yarn.sls.utils.NMUtils;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.sls.utils.SchedulerUtils;
import org.apache.hadoop.yarn.sls.web.SLSWebApp;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TestScheduler {

  private File tempDir;
  private File slsOutputDir;
  private FileAppender fa;

  private static Path yarnSitePath;
  private static Path fairSchedulerPath;
  private static Path capacitySchedulerPath;
  private static Path nodeLabelsPath;
  private static Path slsRunnerPath;
  private static String testClassesPath = TestScheduler.class.getProtectionDomain().getCodeSource().getLocation().getFile();

  private static String yarnSiteXml;
  private static String slsRunnerXml;

  private static Set<String> labels;

  private static final Logger LOG = Logger.getLogger(TestScheduler.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    yarnSitePath = Paths.get(testClassesPath + "/yarn-site.xml");
    fairSchedulerPath = Paths.get(testClassesPath + "/fair-scheduler.xml");
    capacitySchedulerPath = Paths.get(testClassesPath + "/capacity-scheduler.xml");
    nodeLabelsPath = Paths.get(testClassesPath + "/node.labels");
    slsRunnerPath = Paths.get(testClassesPath + "/sls-runner.xml");

    slsRunnerXml = new String(Files.readAllBytes(slsRunnerPath));
    Set<String> slsProperties = SLSUtils.generateSlsRunnerConfiguration(200_000, 24, 24.0,
            4096, 1, 1.0,
            1500);
    String newSlsRunnerXml = insertIntoString(slsRunnerXml, "</configuration>",
            StringUtils.join(slsProperties, "\n"));
    Files.write(slsRunnerPath, newSlsRunnerXml.getBytes());


    yarnSiteXml = new String(Files.readAllBytes(yarnSitePath));
    String property = SchedulerUtils.generateProperty(
            "yarn.nm.liveness-monitor.expiry-interval-ms",
            "2400000");
    String newYarnSiteXml = insertIntoString(yarnSiteXml, "</configuration>", property);
    Files.write(yarnSitePath, newYarnSiteXml.getBytes());

    labels = new HashSet<>();
    labels.add("test");
    labels.add("prod");
    labels.add("dev");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Files.write(yarnSitePath, yarnSiteXml.getBytes());
    Files.write(slsRunnerPath, slsRunnerXml.getBytes());
  }

  @Before
  public void setUp() throws Exception {
    String dirName = UUID.randomUUID().toString();
    tempDir = new File("target", dirName);
    slsOutputDir = new File(tempDir.getAbsolutePath() + "/slsoutput/");

    // add FileAppender to logger so results and logs will be saved is the same directory
    fa = new FileAppender();
    fa.setName("FileLogger");
    fa.setFile(tempDir.getAbsolutePath() + "/" + dirName + ".log");
    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
    fa.setThreshold(Level.INFO);
    fa.setAppend(true);
    fa.activateOptions();
    Logger.getRootLogger().addAppender(fa);
  }

  @After
  public void tearDown() throws Exception {
    SLSRunner slsRunner = (SLSRunner) FieldUtils.readStaticField(SLSRunner.class, "sls", true);
    ResourceManager rm = (ResourceManager) FieldUtils.readDeclaredField(slsRunner, "rm", true);
    ResourceSchedulerWrapper resourceScheduler = (ResourceSchedulerWrapper) rm.getResourceScheduler();
    SLSWebApp web = (SLSWebApp) FieldUtils.readDeclaredField(resourceScheduler, "web", true);

    stopServices(rm, resourceScheduler, web);

    deleteTempFiles();

    LOG.info("Results and logs available at " + tempDir.getAbsolutePath());
    Logger.getRootLogger().removeAppender(fa);
  }

  @Test
  public void testFairScheduler() throws Exception {
    int numQueues = 30;
    int numNodes = 50;
    int numJobs = 30;
    int numTasks = 200;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 25_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(8192, 1, 1.0);

    SchedulerUtils.setScheduler(yarnSitePath, "fair.FairScheduler");
    String fairSchedulerXml = SchedulerUtils.generateFairSchedulerXml(1000, 1000, 50, numQueues, "fair", 2, null);
    Files.write(fairSchedulerPath, fairSchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
  }

  @Test
  public void testCapacityScheduler() throws Exception {
    int numQueues = 30;
    int numNodes = 50;
    int numJobs = 30;
    int numTasks = 200;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 45_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(8192, 1, 1.0);

    SchedulerUtils.setScheduler(yarnSitePath, "capacity.CapacityScheduler");
    String capacitySchedulerXml = SchedulerUtils.generateCapacitySchedulerXml(numQueues, 100, 10_000, null);
    Files.write(capacitySchedulerPath, capacitySchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
  }

  @Test
  public void testFairSchedulerNMRestart() throws Exception {
    int numQueues = 10;
    int numNodes = 10;
    int numJobs = 30;
    int numTasks = 100;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 10_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(4096, 1, 1.0);


    SchedulerUtils.setScheduler(yarnSitePath, "fair.FairScheduler");
    String fairSchedulerXml = SchedulerUtils.generateFairSchedulerXml(10_000, 10_000, 50, numQueues, "fair", 2, null);
    Files.write(fairSchedulerPath, fairSchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // restart nodes
    Thread.sleep(5_000);
    NMUtils.stopNodes();
    Thread.sleep(10_000);
    NMUtils.restartNodes();

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
  }

  @Test
  public void testCapacitySchedulerNMRestart() throws Exception {
    int numQueues = 10;
    int numNodes = 10;
    int numJobs = 10;
    int numTasks = 100;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 10_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(4096, 1, 1.0);

    SchedulerUtils.setScheduler(yarnSitePath, "capacity.CapacityScheduler");
    String capacitySchedulerXml = SchedulerUtils.generateCapacitySchedulerXml(numQueues, 100, 10_000, null);
    Files.write(capacitySchedulerPath, capacitySchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // restart nodes
    Thread.sleep(5_000);
    NMUtils.stopNodes();
    Thread.sleep(5_000);
    NMUtils.restartNodes();

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
  }

  @Test
  public void testFairSchedulerLBS() throws Exception {
    int numQueues = 10;
    int numNodes = 10;
    int numJobs = 10;
    int numTasks = 100;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 10_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(4096, 1, 1.0);

    String nodeLabels = SchedulerUtils.generateNodeLabels(numNodes, labels);
    Files.write(nodeLabelsPath, nodeLabels.getBytes());

    String yarnSiteXml = new String(Files.readAllBytes(yarnSitePath));
    String properties =
            SchedulerUtils.generateProperty("node.labels.file", nodeLabelsPath.toString()) + "\n" +
                    SchedulerUtils.generateProperty("node.labels.monitor.interval", "120000");
    String appendNodeLabelsConfiguration = insertIntoString(yarnSiteXml, "</configuration>", properties);
    Files.write(yarnSitePath, appendNodeLabelsConfiguration.getBytes());

    SchedulerUtils.setScheduler(yarnSitePath, "fair.FairScheduler");
    String fairSchedulerXml = SchedulerUtils.generateFairSchedulerXml(10_000, 10_000, 50, numQueues, "fair", 2, labels);
    Files.write(fairSchedulerPath, fairSchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
    Files.write(yarnSitePath, yarnSiteXml.getBytes());
  }

  @Test
  public void testCapacitySchedulerLBS() throws Exception {
    int numQueues = 10;
    int numNodes = 10;
    int numJobs = 10;
    int numTasks = 100;
    List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 10_000, numQueues, numNodes, true, false);
    JobUtils.setMRAMSimulatorParameters(4096, 1, 1.0);

    String nodeLabels = SchedulerUtils.generateNodeLabels(numNodes, labels);
    Files.write(nodeLabelsPath, nodeLabels.getBytes());

    String yarnSiteXml = new String(Files.readAllBytes(yarnSitePath));
    String properties =
            SchedulerUtils.generateProperty("node.labels.file", nodeLabelsPath.toString()) + "\n" +
                    SchedulerUtils.generateProperty("node.labels.monitor.interval", "120000");
    String appendNodeLabelsConfiguration = insertIntoString(yarnSiteXml, "</configuration>", properties);
    Files.write(yarnSitePath, appendNodeLabelsConfiguration.getBytes());

    SchedulerUtils.setScheduler(yarnSitePath, "capacity.CapacityScheduler");
    String capacitySchedulerXml = SchedulerUtils.generateCapacitySchedulerXml(numQueues, 75, 50, labels);
    Files.write(capacitySchedulerPath, capacitySchedulerXml.getBytes());

    final List<Throwable> exceptionList =
            Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    startTheSimulator(jobs);

    // wait until there are no running apps left
    while (isRemainingApps()) {
      if (checkExceptions(exceptionList)) break;
    }
    Files.write(yarnSitePath, yarnSiteXml.getBytes());
  }

  private void startTheSimulator(List<JobUtils.Job> jobs) throws Exception {
    String inputTracesPath = JobUtils.generateInputJobTraces(jobs, tempDir).getAbsolutePath();
    String args[] = new String[]{
            "-inputsls", inputTracesPath,
            "-output", slsOutputDir.getAbsolutePath()};
    SLSRunner.main(args);
  }

  private void stopServices(ResourceManager rm, ResourceSchedulerWrapper resourceScheduler, SLSWebApp web) throws Exception {
    rm.stop();
    resourceScheduler.serviceStop();

    Method[] declaredMethods = ResourceSchedulerWrapper.class.getDeclaredMethods();
    for (Method method : declaredMethods) {
      if (method.getName().equals("tearDown")) {
        method.setAccessible(true);
        method.invoke(resourceScheduler, null);
      }
    }
    web.stop();
  }

  private boolean isRemainingApps() throws IllegalAccessException {
    return (int) FieldUtils.readStaticField(SLSRunner.class, "remainingApps", true) > 0;
  }

  private boolean checkExceptions(List<Throwable> exceptionList) {
    if (!exceptionList.isEmpty()) {
      SLSRunner.getRunner().stop();
      for (Throwable t : exceptionList) {
        LOG.error(t);
      }
      Assert.fail("TestSLSRunner catched exception from child thread " +
              "(TaskRunner.Task): " + exceptionList.get(0).getMessage());
      return true;
    }
    return false;
  }

  private static void deleteTempFiles() {
    try {
      Files.deleteIfExists(fairSchedulerPath);
      Files.deleteIfExists(capacitySchedulerPath);
      Files.deleteIfExists(nodeLabelsPath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String insertIntoString(String source, String before, String value) {
    int index = source.lastIndexOf(before);
    return source.substring(0, index - 1) + value + source.substring(index);
  }
}