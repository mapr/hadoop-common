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

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestJobUtils {

  @Test
  public void generateJobTraces() throws IOException {
    File tempDir = new File("target", UUID.randomUUID().toString());
    if (!tempDir.exists()
            && !tempDir.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
              + tempDir.getAbsolutePath());
      System.exit(1);
    }
    JobUtils.Job job = new JobUtils.Job("mapreduce", 0, 100, "queue_1", "job_1", "default", new ArrayList<JobUtils.Task>());

    File jobTraces = JobUtils.generateInputJobTraces(Collections.singletonList(job), tempDir);

    Assert.assertTrue(jobTraces.exists());
    List<String> jobLines = Files.readAllLines(Paths.get(jobTraces.getAbsolutePath()), Charset.defaultCharset());
    Assert.assertTrue(!jobLines.isEmpty());
    FileUtils.deleteDirectory(tempDir);
  }
}