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

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.BaseSLSRunnerTest;
import org.apache.hadoop.yarn.sls.utils.JobUtils;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.sls.utils.SchedulerUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@RunWith(value = Parameterized.class)
@NotThreadSafe
public class TestScheduler extends BaseSLSRunnerTest {

    private static String capScheduler = CapacityScheduler.class.getCanonicalName();
    private static String fairScheduler = FairScheduler.class.getCanonicalName();
    private static Path fairSchedulerPath;
    private static Path capacitySchedulerPath;
    private static Path slsRunnerPath;
    private static String testClassesPath = TestScheduler.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    private static String slsRunnerXml;

    private static File tempDir;

    private static int numQueues;

    @Parameterized.Parameters(name = "Testing with: {1}, {0}, (nodeFile {3})")
    public static Collection<Object[]> data() {
        numQueues = 30;
        int numNodes = 20;
        int numJobs = 30;
        int numTasks = 200;
        String dirName = UUID.randomUUID().toString();
        File tempDir = new File("target", dirName);
        List<JobUtils.Job> jobs = JobUtils.generateJobList(numJobs, 0, 200, numTasks, 0, 25_000, numQueues, numNodes, true, false);
        String inputTracesPath = JobUtils.generateInputJobTraces(jobs, tempDir).getAbsolutePath();

        String nodeFile = "src/test/resources/nodes_extend.json";
        return Arrays.asList(new Object[][] {
                {fairScheduler, "SLS",
                        inputTracesPath, nodeFile},
                {capScheduler, "SLS",
                        inputTracesPath, nodeFile}
        });
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        tempDir = new File("target", UUID.randomUUID().toString());
        fairSchedulerPath = Paths.get(testClassesPath + "/fair-scheduler.xml");
        capacitySchedulerPath = Paths.get(testClassesPath + "/capacity-scheduler.xml");
        slsRunnerPath = Paths.get(testClassesPath + "/sls-runner.xml");

        slsRunnerXml = new String(Files.readAllBytes(slsRunnerPath));
        Set<String> slsProperties = SLSUtils.generateSlsRunnerConfiguration(200_000, 24,
                4096, 1,
                1500);
        String newSlsRunnerXml = insertIntoString(slsRunnerXml, "</configuration>",
                StringUtils.join(slsProperties, "\n"));
        Files.write(slsRunnerPath, newSlsRunnerXml.getBytes());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        Files.write(slsRunnerPath, slsRunnerXml.getBytes());
        deleteTempFiles();
    }

    @Override
    public void setup() {
    }

    @Test
    public void testScheduler() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("scheduler.sls.test", "true");
        conf.set(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, "2400000");
        conf.set(YarnConfiguration.RM_STORE, "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore");
        conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI, tempDir.getAbsolutePath() + "/fsStore");
        conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
        long timeTillShutdownInsec = 120L;
        if(schedulerType.equals(fairScheduler)){
            String fairSchedulerXml = SchedulerUtils.generateFairSchedulerXml(1000, 1000, 50, numQueues, "fair", 2, null);
            Files.write(fairSchedulerPath, fairSchedulerXml.getBytes());
        } else {
            String capacitySchedulerXml = SchedulerUtils.generateCapacitySchedulerXml(numQueues, 100, 10_000, null);
            Files.write(capacitySchedulerPath, capacitySchedulerXml.getBytes());
        }
        runSLS(conf, timeTillShutdownInsec);
    }

    private static void deleteTempFiles() {
        try {
            Files.deleteIfExists(fairSchedulerPath);
            Files.deleteIfExists(capacitySchedulerPath);
            Files.delete(tempDir.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String insertIntoString(String source, String before, String value) {
        int index = source.lastIndexOf(before);
        return source.substring(0, index - 1) + value + source.substring(index);
    }
}