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

package org.apache.hadoop.mapreduce.conf;

import org.apache.hadoop.mapred.MapRFsOutputBuffer;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.MapRIFileInputStream;
import org.apache.hadoop.mapred.MapRIFileOutputStream;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.task.reduce.DirectShuffle;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MapReduceDefaultProperties extends Properties {
  private static final Map<String, String> props =
    new HashMap<String, String>();

  static { // MapReduce framework related defaults
    props.put(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);

    // TODO Refer to YarnDefaultProperties.DEFAULT_RM_STAGING_DIR once this
    // file is moved out to MapR code base.
    props.put(MRJobConfig.MR_AM_STAGING_DIR, "${fs.defaultFS}/var/mapr/cluster/yarn/rm/staging");
  }

  static { // Direct shuffle configuration
    props.put(MRConfig.TASK_LOCAL_OUTPUT_CLASS, MapRFsOutputFile.class.getName());
    props.put(MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, MapRFsOutputBuffer.class.getName());
    props.put(MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, MapRFsOutputBuffer.class.getName());
    props.put(MRConfig.SHUFFLE_CONSUMER_PLUGIN, DirectShuffle.class.getName());
    props.put(MRConfig.MAPRED_IFILE_OUTPUTSTREAM, MapRIFileOutputStream.class.getName());
    props.put(MRConfig.MAPRED_IFILE_INPUTSTREAM, MapRIFileInputStream.class.getName());
    props.put(MRConfig.MAPRED_LOCAL_MAP_OUTPUT, "false");
    props.put(MRJobConfig.MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES, "mapr_direct_shuffle");

    props.put("mapr.mapred.localvolume.root.dir.name", "nodeManager");
    props.put("mapr.localoutput.dir", "output");
    props.put("mapr.localspill.dir", "spill");

    // Configurations for MapRFSOutputBuffer
    props.put("mapred.maxthreads.generate.mapoutput", "1");
    props.put("mapred.maxthreads.partition.closer", "1");
    props.put("mapr.map.keyprefix.ints", "1");
  }

  static { // Map side defaults
    props.put(MRJobConfig.MAP_OUTPUT_COMPRESS, "false");
    props.put(MRJobConfig.MAP_SPECULATIVE, "true");

  }

  static { // Reduce side defaults
    props.put(MRJobConfig.REDUCE_SPECULATIVE, "true");
  }

  static { // Map side performance tuning defaults
    props.put(MRJobConfig.MAP_MEMORY_MB, "1024");
    props.put(MRJobConfig.MAP_CPU_VCORES, "1");
    props.put(MRJobConfig.MAP_DISK, "0.5");
    props.put(MRJobConfig.MAP_JAVA_OPTS, "-Xmx900m");
    props.put(MRJobConfig.IO_SORT_MB, getIoSortMb());
    props.put(MRJobConfig.IO_SORT_FACTOR, "256");
    props.put(MRJobConfig.MAP_SORT_SPILL_PERCENT, "0.99");

    // TODO: We should remove this as this is removed in MR2 as part of MAPREDUCE-64.
    props.put("io.sort.record.percent", "0.17");
  }

  static { // Reduce side performance tuning defaults
    props.put(MRJobConfig.REDUCE_MEMORY_MB, "3072");
    props.put(MRJobConfig.REDUCE_CPU_VCORES, "1");
    props.put(MRJobConfig.REDUCE_DISK, "1.33");
    props.put(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560m");
    props.put(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, "1.00");
    props.put(MRJobConfig.SHUFFLE_PARALLEL_COPIES, "12");
  }
  
  static { // Set mapreduce job history http policy
    String http_scheme = HttpConfig.Policy.HTTP_ONLY.name();
    if(UserGroupInformation.isSecurityEnabled()==true) {
      http_scheme = HttpConfig.Policy.HTTPS_ONLY.name();
    }
    props.put(JHAdminConfig.MR_HS_HTTP_POLICY, http_scheme);
  }

  private static final long IO_SORT_XMX_THRESHOLD = 800 << 20;
  private static final String IO_SORT_MB_MIN = "100";
  private static final String IO_SORT_MB_MAX = "480";

  private static String getIoSortMb() {
    return Runtime.getRuntime().maxMemory() >= IO_SORT_XMX_THRESHOLD ? IO_SORT_MB_MAX : IO_SORT_MB_MIN;
  }

  public MapReduceDefaultProperties() {
    this.putAll(props);
  }

}
