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
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.Random;

public class CentralConfigUpdaterThread extends Thread {
  private static final Log LOG = 
    LogFactory.getLog(CentralConfigUpdaterThread.class);
  private static final long CENTRAL_CONFIG_CHECK_INTERVAL = 60*1000;
  private FileSystem maprfs = null;
  private Path remoteFilePath = null; 
  private Path localFilePath = null;
  private boolean shutdown = false;
  private boolean initialSleep = true;

  public CentralConfigUpdaterThread(FileSystem fs, Path remoteFilePath,
     Path localFilePath) throws IOException {
    // careful initialization
    if (fs != null) 
      this.maprfs = fs;
    else 
      throw new IOException("NULL FileSystem");
    if (remoteFilePath != null) 
      this.remoteFilePath = remoteFilePath;
    else 
      throw new IOException("NULL remoteFile");
    if (localFilePath != null) 
      this.localFilePath = localFilePath;
    else
      throw new IOException("NULL localFile");
  }

  public void shutdown() {
    this.shutdown  = true;
  }

  private void copyCentralConfig() throws IOException {
    if (!maprfs.exists(remoteFilePath)) 
      return;
    File localFile = new File(localFilePath.toString());
    long remoteMtime = maprfs.getFileStatus(remoteFilePath).getModificationTime();
    remoteMtime = (remoteMtime/1000) * 1000;
    if (localFile.exists()) {
      long localMtime = localFile.lastModified();
      if (localMtime == remoteMtime) {
        return;
      }
    }
    if (LOG.isInfoEnabled())
      LOG.info("Copying " + remoteFilePath + " to " + localFilePath);
    maprfs.copyToLocalFile(remoteFilePath, localFilePath);
    localFile = new File(localFilePath.toString());
    if (localFile.exists()) {
      localFile.setLastModified(remoteMtime);
    }
  }


  @Override 
  public void run() {
    LOG.info("Starting thread " + this.getName());
    while (!shutdown) {
      try {
        copyCentralConfig();
        // sleep for a while 0-15sec
        if (initialSleep) {
          sleep(new Random().nextInt(15000));
          initialSleep = false;
        }
        sleep(CENTRAL_CONFIG_CHECK_INTERVAL);
      } catch (InterruptedException ie) {
        if (!shutdown) {
          LOG.info("Unexpected InterruptedException..Continuing");
        } else {
          break;
        }
      } catch (Exception e) {
        LOG.warn("Error in Thread " + this.getName() + " " + e + " .Exiting.");
        break;
      }
    }
  }
}
