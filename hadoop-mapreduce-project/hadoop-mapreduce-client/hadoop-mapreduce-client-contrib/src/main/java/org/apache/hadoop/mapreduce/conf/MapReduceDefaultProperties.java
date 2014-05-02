package org.apache.hadoop.mapreduce.conf;

import org.apache.hadoop.mapred.MapRFsOutputBuffer;
import org.apache.hadoop.mapred.MapRFsOutputFile;
import org.apache.hadoop.mapred.MapRIFileInputStream;
import org.apache.hadoop.mapred.MapRIFileOutputStream;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.task.reduce.DirectShuffle;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MapReduceDefaultProperties extends Properties {
  private static final Map<String, String> props =
      new HashMap<String, String>();

  static { // MapReduce framework related defaults
    props.put(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    props.put(MRJobConfig.MR_AM_STAGING_DIR, "${fs.defaultFS}/tmp/staging");
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
    props.put(MRJobConfig.MAP_JAVA_OPTS, "-Xmx900m");
    props.put(MRJobConfig.IO_SORT_MB, "480");
    props.put(MRJobConfig.IO_SORT_FACTOR, "256");
    props.put(MRJobConfig.MAP_SORT_SPILL_PERCENT, "0.99");
  }

  static { // Reduce side performance tuning defaults
    props.put(MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx1500m");
    props.put(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, "0.95");
    props.put(MRJobConfig.SHUFFLE_PARALLEL_COPIES, "12");
  }

  public MapReduceDefaultProperties() {
    this.putAll(props);
  }

}
