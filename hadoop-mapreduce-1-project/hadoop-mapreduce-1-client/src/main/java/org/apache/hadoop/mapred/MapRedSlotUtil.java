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

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.Math;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.MapRConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.MapRedSlotUtil;
import org.apache.hadoop.mapred.MapRedSlotUtil.MapRedSlotInfo;
import org.apache.log4j.Logger;

public class MapRedSlotUtil {
  
  public static class MapRedSlotInfo {
    public int mapTaskMem, reduceTaskMem;
    public int maxMapSlots, maxReduceSlots;
  }

  public static class MachineInfo {
    private int cpuCount, mfsCpuCount, diskCount, mfsDiskCount;
    private long reservedMemInMB;

    public MachineInfo(int cpuCount, int diskCount, 
                       int mfsCpuCount, int mfsDiskCount, long reservedMemInMB) {
       this.cpuCount = cpuCount;
       this.diskCount = diskCount;
       this.mfsCpuCount = mfsCpuCount;
       this.mfsDiskCount = mfsDiskCount;
       this.reservedMemInMB = reservedMemInMB;
    }

    public int getCpuCount() {
       return cpuCount;
    }

    public int getMfsCpuCount() {
       return mfsCpuCount;
    }

    public int getDiskCount() {
       return diskCount;
    }

    public int getMfsDiskCount() {
       return mfsDiskCount;
    }

    public long getReservedMemory() {
       return reservedMemInMB;
    }
  }

  public static final long DEFAULT_CHUNKSIZE_BYTES_FOR_SLOTS = 268435456L;
  public static final int  DEFAULT_MEMORY_MB_FOR_MAP_TASK = 1024;    // 1G
  public static final int  DEFAULT_MEMORY_MB_FOR_REDUCE_TASK = 3072; // 3G

  public static final String MAPR_HOME = System.getProperty("mapr.home.dir", "/opt/mapr");
  public static final String RESOURCES_FILE = "/logs/cpu_mem_disk";

  private static final Log LOG = LogFactory.getLog(MapRedSlotUtil.class);

  /**
   * Adjust Map and Reduce slots
   * @param conf the TT configuration
   * @param mInfo the machine CPU/Mem/Disk information
   * @return an object with adjusted map/reduce slots and map/reduce task memory
   */
  public static MapRedSlotInfo adjustSlots(Configuration conf, MachineInfo mInfo) {
    
    MapRedSlotInfo mrInfo = new MapRedSlotInfo();
    
    mrInfo.mapTaskMem = conf.getInt("mapred.maptask.memory.default", 
                    DEFAULT_MEMORY_MB_FOR_MAP_TASK);
    if (mrInfo.mapTaskMem < 0L)
      mrInfo.mapTaskMem = DEFAULT_MEMORY_MB_FOR_MAP_TASK;

    mrInfo.reduceTaskMem = conf.getInt("mapred.reducetask.memory.default", 
                    DEFAULT_MEMORY_MB_FOR_REDUCE_TASK);
    if (mrInfo.reduceTaskMem < 0L)
      mrInfo.reduceTaskMem = DEFAULT_MEMORY_MB_FOR_REDUCE_TASK;

    // For chunksize < 256M, we use half the map/reduce memory as default case
    try {
      FileSystem fs = FileSystem.get(conf);
      long chunkSize = fs.getDefaultBlockSize();
      if (chunkSize < DEFAULT_CHUNKSIZE_BYTES_FOR_SLOTS) {
        if (mrInfo.mapTaskMem == DEFAULT_MEMORY_MB_FOR_MAP_TASK) {
          mrInfo.mapTaskMem = DEFAULT_MEMORY_MB_FOR_MAP_TASK >> 1; // Use half
        }
        if (mrInfo.reduceTaskMem == DEFAULT_MEMORY_MB_FOR_REDUCE_TASK) {
          mrInfo.reduceTaskMem = DEFAULT_MEMORY_MB_FOR_REDUCE_TASK >> 1; // Use half
        }
      } 
    } catch (Exception e) {
      LOG.error("Exception while fetching chunksize " + e.getMessage());
    }

    mrInfo.maxReduceSlots = (int)
          (((1 - MapRConf.MAPTASK_MEM_RATIO) * mInfo.getReservedMemory()) / mrInfo.reduceTaskMem);
    mrInfo.maxMapSlots = (int)
          ((mInfo.getReservedMemory() - ( mrInfo.maxReduceSlots * mrInfo.reduceTaskMem)) / mrInfo.mapTaskMem);
    
    if (LOG.isInfoEnabled()) {
      LOG.info("Before adjustment, maxMapSlots = " + mrInfo.maxMapSlots + 
               ", maxReduceSlots = " + mrInfo.maxReduceSlots);
    }

    // Cap slots with cpu Count    
    if (mInfo.getCpuCount() > 0) { // sanity check
      int mSlots = 1, rSlots = 1;
      if (mInfo.getCpuCount() > mInfo.getMfsCpuCount()) {
        rSlots = mInfo.getCpuCount() - mInfo.getMfsCpuCount();
      } else if (mInfo.getCpuCount() == mInfo.getMfsCpuCount()) {
        rSlots = 1;
      } else {
        // This cannot happen (mfsCpuCount < cpuCount). But still cover this case
        rSlots = mInfo.getCpuCount();
      }
      mSlots = rSlots * 2;
      if (mrInfo.maxReduceSlots > rSlots) mrInfo.maxReduceSlots = rSlots;
      if (mrInfo.maxMapSlots > mSlots) mrInfo.maxMapSlots = mSlots;

      if (LOG.isInfoEnabled()) {
        LOG.info("After CPU adjustment, maxMapSlots = " + mrInfo.maxMapSlots + 
                 ", maxReduceSlots = " + mrInfo.maxReduceSlots);
      }
    }

    // Cap slots with mfsDiskCount
    if (mInfo.getMfsDiskCount() > 0) { // sanity check
      int mSlots = 2 * mInfo.getMfsDiskCount();
      int rSlots = mInfo.getMfsDiskCount() > 2 ? (int)(0.75 * mInfo.getMfsDiskCount()): 1;

      if (mrInfo.maxReduceSlots > rSlots) mrInfo.maxReduceSlots = rSlots;
      if (mrInfo.maxMapSlots > mSlots) mrInfo.maxMapSlots = mSlots;
      
      if (LOG.isInfoEnabled()) {
        LOG.info("After Disk adjustment, maxMapSlots = " + mrInfo.maxMapSlots + 
                 ", maxReduceSlots = " + mrInfo.maxReduceSlots);
      }
    }
    
    if (mrInfo.maxReduceSlots <= 0)
       mrInfo.maxReduceSlots = 1;

    if (mrInfo.maxMapSlots <= 0)
       mrInfo.maxMapSlots = 1;

    if (LOG.isInfoEnabled()) {
      LOG.info("After adjustment, maxMapSlots = " + mrInfo.maxMapSlots + 
               ", maxReduceSlots = " + mrInfo.maxReduceSlots);
      LOG.info("mapTaskMem = " + mrInfo.mapTaskMem + 
               ", reduceTaskMem = " + mrInfo.reduceTaskMem);
    }
    
    return mrInfo;
  }
  
  /**
   * Adjust Map and Reduce slots
   * @param conf the TT configuration
   * @param reservedMemInMB the memory reserved for map and reduce tasks on this machine
   * @return an object with adjusted map/reduce slots and map/reduce task memory
   */
  public static long adjustSlots(Configuration conf, long reservedMemInMB) {

    // Get system resources (CPU/Mem/Disks)
    MachineInfo mInfo = getResourceInfo(reservedMemInMB);
    if (mInfo != null) {
      MapRedSlotInfo mrInfo = adjustSlots(conf, mInfo);
      return (mrInfo.maxMapSlots * mrInfo.mapTaskMem) + 
              (mrInfo.maxReduceSlots * mrInfo.reduceTaskMem);
    }
    return 0L;
  }
  
  /* Read resource info from /opt/mapr/logs/cpu_mem_disk */
  private static MachineInfo getResourceInfo(long reservedMemInMB) {
     String resourcesFile = MAPR_HOME + RESOURCES_FILE;
     if (LOG.isInfoEnabled())
       LOG.info("Loading resource properties file : " + resourcesFile);
     
     Properties properties = new Properties();
     try {
       properties.load(new FileInputStream(resourcesFile));
     } catch (Exception e) {
       /* On any exception while reading resourcesFile return null */
       LOG.warn("Error while reading " + resourcesFile + " : ", e);
       return null;
     }

     int cpuCount = Integer.valueOf(properties.getProperty("cpus", "0"));
     int diskCount = Integer.valueOf(properties.getProperty("disks", "0"));
     int mfsCpuCount = Integer.valueOf(properties.getProperty("mfscpus", "0"));
     int mfsDiskCount = Integer.valueOf(properties.getProperty("mfsdisks", "0"));
     
     MachineInfo mInfo = new MachineInfo(cpuCount, diskCount, 
                                mfsCpuCount, mfsDiskCount, reservedMemInMB);

     if (LOG.isInfoEnabled()) {
       LOG.info("CPUS: " + cpuCount);
       LOG.info("mfsCPUS: " + mfsCpuCount);
       LOG.info("DISKS: " + diskCount);
       LOG.info("mfsDISKS: " + mfsDiskCount);
     }
     return mInfo;
  }
}
