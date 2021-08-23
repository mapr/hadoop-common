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

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.MapRCommonSecurityUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MapReduceDefaultProperties extends Properties {

  public static final String CLUSTER_NAME_PREFIX = "cluster.name.prefix";
  private static final Map<String, String> props =
    new HashMap<String, String>();

  static { // MapReduce framework related defaults
    props.put(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);

    props.put(MRJobConfig.MR_AM_STAGING_DIR, "${yarn.resourcemanager.staging}");

  }

  static { // Direct shuffle configuration
    props.put(MRConfig.TASK_LOCAL_OUTPUT_CLASS, "org.apache.hadoop.mapred.MapRFsOutputFile");
    props.put(MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, "org.apache.hadoop.mapred.MapRFsOutputBuffer");
    props.put(MRConfig.SHUFFLE_CONSUMER_PLUGIN, "org.apache.hadoop.mapreduce.task.reduce.DirectShuffle");
    props.put(MRConfig.MAPRED_IFILE_OUTPUTSTREAM, "org.apache.hadoop.mapred.MapRIFileOutputStream");
    props.put(MRConfig.MAPRED_IFILE_INPUTSTREAM, "org.apache.hadoop.mapred.MapRIFileInputStream");
    props.put(MRConfig.MAPRED_LOCAL_MAP_OUTPUT, "false");
    props.put(MRJobConfig.MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES, "mapr_direct_shuffle");

    final String name; 
    if ( System.getProperty(CLUSTER_NAME_PREFIX) != null ) {
      props.put(CLUSTER_NAME_PREFIX, System.getProperty(CLUSTER_NAME_PREFIX));
      name = System.getProperty(CLUSTER_NAME_PREFIX) + "/nodeManager";
    } else {
      name = "nodeManager";
    } 
    props.put("mapr.mapred.localvolume.root.dir.name", name); 
  
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


  static { // MapReduce env configuration
    props.put(MRJobConfig.MR_AM_ENV, "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
    props.put(MRJobConfig.MAP_ENV, "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
    props.put(MRJobConfig.REDUCE_ENV, "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
  }

  static { // Map side performance tuning defaults
    props.put(MRJobConfig.MAP_MEMORY_MB, "1024");
    props.put(MRJobConfig.MAP_CPU_VCORES, "1");
    props.put(MRJobConfig.MAP_RESOURCE_TYPE_PREFIX + "disks", "500");
    props.put(MRJobConfig.MAP_JAVA_OPTS, "-Xmx900m --add-opens java.base/java.lang=ALL-UNNAMED");
    props.put(MRJobConfig.IO_SORT_MB, getIoSortMb());
    props.put(MRJobConfig.IO_SORT_FACTOR, "256");
    props.put(MRJobConfig.MAP_SORT_SPILL_PERCENT, "0.99");

    // TODO: We should remove this as this is removed in MR2 as part of MAPREDUCE-64.
    props.put("io.sort.record.percent", "0.17");
  }

  static { // Reduce side performance tuning defaults
    props.put(MRJobConfig.REDUCE_MEMORY_MB, "3072");
    props.put(MRJobConfig.REDUCE_CPU_VCORES, "1");
    props.put(MRJobConfig.REDUCE_RESOURCE_TYPE_PREFIX + "disks", "1000");
    props.put(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560m --add-opens java.base/java.lang=ALL-UNNAMED");
    props.put(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, "1.00");
    props.put(MRJobConfig.SHUFFLE_PARALLEL_COPIES, "12");
  }
  
  static { // Set mapreduce job history http policy
    String http_scheme = HttpConfig.Policy.HTTP_ONLY.name();
    if(MapRCommonSecurityUtil.getInstance().isSecurityEnabled()==true) {
      http_scheme = HttpConfig.Policy.HTTPS_ONLY.name();
    }
    props.put("mapreduce.jobhistory.http.policy", http_scheme);
  }

  private static final long IO_SORT_XMX_THRESHOLD = 800 << 20;
  private static final String IO_SORT_MB_MIN = "100";
  private static final String IO_SORT_MB_MAX = "480";

  private static String getIoSortMb() {
    return Runtime.getRuntime().maxMemory() >= IO_SORT_XMX_THRESHOLD ? IO_SORT_MB_MAX : IO_SORT_MB_MIN;
  }

  public static Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(props);
    return properties;
  }

  public MapReduceDefaultProperties() {
    this.putAll(props);
  }

}
