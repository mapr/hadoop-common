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
package org.apache.hadoop.yarn.sls.utils;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.yarn.sls.appmaster.MRAMSimulator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JobUtils {

  /**
   * generate and save locally input traces file from list of Jobs
   * @see Job
   */
  public static File generateInputJobTraces(List<Job> jobs, File slsOutputDir) {
    ObjectMapper mapper = new ObjectMapper();
    File jobTraces = new File(slsOutputDir.getAbsolutePath() + "/jobTraces.json");
    try {
      if ((!slsOutputDir.exists() && !slsOutputDir.mkdirs()) || !jobTraces.createNewFile()) {
        System.err.println("ERROR: Cannot create job traces file "
                + jobTraces.getAbsolutePath());
        System.exit(1);
      }
      for (Job job : jobs) {
        String jsonJob = mapper.writeValueAsString(job);
        Files.write(Paths.get(jobTraces.getAbsolutePath()), jsonJob.getBytes(), StandardOpenOption.APPEND);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return jobTraces;
  }

  public static List<Job> generateJobList(int numJobs, int jobStartMs, int jobEndMs,
                                          int numTasks, int containerStartMs, int containerEndMs,
                                          int numQueues, int numNodes,
                                          boolean random, boolean increasingly) {
    List<Job> jobs = new ArrayList<>();
    Random rand = new Random();

    // generate jobs
    for (int jobIndex = 1; jobIndex <= numJobs; jobIndex++) {
      List<Task> tasks = new ArrayList<>();

      // generate task list (unique for every job)
      for (int taskIndex = 1; taskIndex <= numTasks; taskIndex++) {
        // randomize values
        int taskDuration = containerEndMs - containerStartMs;
        int taskStart = containerStartMs == 0 ? containerStartMs :
                random ? rand.nextInt(containerStartMs) : containerStartMs;
        int taskEnd = random ? rand.nextInt(taskDuration) + taskStart : containerEndMs + taskStart;
        int nodeNumber = rand.nextInt(numNodes);
        String containerType = Math.random() > 0.5 ? "map" : "reduce";
        int containerPriority = containerType.equals("map") ? 20 : 10; // map tasks should be first
        String containerHost = "/default-rack/" + nodeNumber;

        Task task = new Task(containerHost,
                increasingly ? taskStart * taskIndex : taskStart,
                increasingly ? taskEnd * taskIndex : taskEnd,
                containerPriority, containerType);
        tasks.add(task);
      }

      // randomize values
      int jobDuration = jobEndMs - jobStartMs;
      int jobStart = random ? rand.nextInt(jobStartMs == 0 ? 1 : jobStartMs) : jobStartMs;
      int jobEnd = random ? rand.nextInt(jobDuration) + jobStart : jobDuration + jobStart;
      int queueNumber = jobIndex % numQueues;
      String queueName = "sls_queue_" + (queueNumber == 0 ? queueNumber + 1 : queueNumber);
      String jobId = "job_" + jobIndex;

      Job job = new Job("mapreduce",
              increasingly ? jobStart * jobIndex : jobStart,
              increasingly ? jobEnd * jobIndex : jobEnd,
              queueName, jobId, "default", tasks);
      jobs.add(job);
    }
    return jobs;
  }

  public static void setMRAMSimulatorParameters(int memory, int vcores, double disks) {
    try {
      FieldUtils.writeStaticField(MRAMSimulator.class, "MR_AM_CONTAINER_RESOURCE_MEMORY_MB", memory, true);
      FieldUtils.writeStaticField(MRAMSimulator.class, "MR_AM_CONTAINER_RESOURCE_VCORES", vcores, true);
      FieldUtils.writeStaticField(MRAMSimulator.class, "MR_AM_CONTAINER_RESOURCE_DISKS", disks, true);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  public static class Job {
    @JsonProperty("am.type")
    private String amType;
    @JsonProperty("job.start.ms")
    private int jobStartMs;
    @JsonProperty("job.end.ms")
    private int jobEndMs;
    @JsonProperty("job.queue.name")
    private String queueName;
    @JsonProperty("job.id")
    private String jobId;
    @JsonProperty("job.user")
    private String jobUser;
    @JsonProperty("job.tasks")
    private List<Task> jobTasks;

    public Job(String amType, int jobStartMs, int jobEndMs, String queueName, String jobId, String jobUser, List<Task> jobTasks) {
      this.amType = amType;
      this.jobStartMs = jobStartMs;
      this.jobEndMs = jobEndMs;
      this.queueName = queueName;
      this.jobId = jobId;
      this.jobUser = jobUser;
      this.jobTasks = jobTasks;
    }

    public String getAmType() {
      return amType;
    }

    public int getJobStartMs() {
      return jobStartMs;
    }

    public int getJobEndMs() {
      return jobEndMs;
    }

    public String getQueueName() {
      return queueName;
    }

    public String getJobId() {
      return jobId;
    }

    public String getJobUser() {
      return jobUser;
    }

    public List<Task> getJobTasks() {
      return jobTasks;
    }
  }

  public static class Task {
    @JsonProperty("container.host")
    private String containerHost;
    @JsonProperty("container.start.ms")
    private int containerStartMs;
    @JsonProperty("container.end.ms")
    private int containerEndMs;
    @JsonProperty("container.priority")
    private int contanierPriority;
    @JsonProperty("container.type")
    private String containerType;

    public Task(String containerHost, int containerStartMs, int containerEndMs, int contanierPriority, String containerType) {
      this.containerHost = containerHost;
      this.containerStartMs = containerStartMs;
      this.containerEndMs = containerEndMs;
      this.contanierPriority = contanierPriority;
      this.containerType = containerType;
    }

    public String getContainerHost() {
      return containerHost;
    }

    public int getContainerStartMs() {
      return containerStartMs;
    }

    public int getContainerEndMs() {
      return containerEndMs;
    }

    public int getContanierPriority() {
      return contanierPriority;
    }

    public String getContainerType() {
      return containerType;
    }
  }
}