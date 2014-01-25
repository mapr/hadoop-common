/**
 * Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.conf;

//import com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils;
//import com.mapr.fs.ShimLoader;
//import com.mapr.security.JNISecurity;
//import com.mapr.baseutils.JVMProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

// mapr_extensibility
public class MapRConf
{
  static {
    // TODO Temporarily commented out
    //ShimLoader.load();
  }

  private static final Log LOG = LogFactory.getLog(MapRConf.class);

  private static final String MAPR_ENV_VAR = "MAPR_HOME";

  private static final String MAPR_PROPERTY_HOME = "mapr.home.dir";

  private static final String MAPR_HOME_PATH_DEFAULT = "/opt/mapr";

  public static final long IO_SORT_XMX_THRESHOLD = 800L << 20;

  public static final float MAPTASK_MEM_RATIO = (float)0.4;

  public static final boolean isSecurityEnabled;

  // TODO Temporarily commented out
  static {
    // TODO - how do we know which cluster???
    //String currentClusterName = CLDBRpcCommonUtils.getInstance().getCurrentClusterName();
    //isSecurityEnabled = JNISecurity.IsSecurityEnabled(currentClusterName);
    isSecurityEnabled = false;
    //JVMProperties.init();
  }
  
  private static final Map<String,String> props =
    new HashMap<String,String>();

  public static final void copyTo(Map<String,String> toMap) {
    toMap.putAll(props);
  }

  public static final int size() {
    return props.size();
  }

  //
  // add property keys in the alphabetic order
  //

  public static final String FS_AUTOMATIC_CLOSE = "true";
  static {
    props.put("fs.automatic.close", // core-default.xml
      FS_AUTOMATIC_CLOSE);
  }
  public static final String FS_CHECKPOINT_DIR = "${hadoop.tmp.dir}/dfs/namesecondary";
  static {
    props.put("fs.checkpoint.dir", // core-defaut.xml
      FS_CHECKPOINT_DIR);
  }
  public static final String FS_CHECKPOINT_EDITS_DIR = "${fs.checkpoint.dir}";
  static {
    props.put("fs.checkpoint.edits.dir", // core-default.xml
      FS_CHECKPOINT_EDITS_DIR);
  }
  public static final String FS_CHECKPOINT_PERIOD = "3600";
  static {
    props.put("fs.checkpoint.period", // core-default.xml
      FS_CHECKPOINT_PERIOD);
  }
  public static final String FS_CHECKPOINT_SIZE = "67108864";
  static {
    props.put("fs.checkpoint.size", // core-default.xml
      FS_CHECKPOINT_SIZE);
  }
  public static final String FS_DEFAULT_NAME = "maprfs:///";
  static {
    props.put("fs.default.name", // core-default.xml
      FS_DEFAULT_NAME);
  }
  public static final String FS_FILE_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
  static {
    props.put("fs.file.impl", // core-default.xml
      FS_FILE_IMPL);
  }
  public static final String FS_FTP_IMPL = "org.apache.hadoop.fs.ftp.FTPFileSystem";
  static {
    props.put("fs.ftp.impl", // core-default.xml
      FS_FTP_IMPL);
  }
  public static final String FS_HAR_IMPL_DISABLE_CACHE = "true";
  static {
    props.put("fs.har.impl.disable.cache", // core-default.xml
      FS_HAR_IMPL_DISABLE_CACHE);
  }
  public static final String FS_HAR_IMPL = "org.apache.hadoop.fs.HarFileSystem";
  static {
    props.put("fs.har.impl", // core-default.xml
      FS_HAR_IMPL);
  }
  public static final String FS_HFTP_IMPL = "org.apache.hadoop.hdfs.HftpFileSystem";
  static {
    props.put("fs.hftp.impl", // core-default.xml
      FS_HFTP_IMPL);
  }
  public static final String FS_HSFTP_IMPL = "org.apache.hadoop.hdfs.HsftpFileSystem";
  static {
    props.put("fs.hsftp.impl", // core-default.xml
      FS_HSFTP_IMPL);
  }
  public static final String FS_KFS_IMPL = "org.apache.hadoop.fs.kfs.KosmosFileSystem";
  static {
    props.put("fs.kfs.impl", // core-default.xml
      FS_KFS_IMPL);
  }
  public static final String FS_MAPRFS_IMPL = "com.mapr.fs.MapRFileSystem";
  static {
    props.put("fs.maprfs.impl", // core-default.xml
      FS_MAPRFS_IMPL);
  }
  public static final String FS_HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";

 // Bug 12079: By default, we map the HDFS Impl to MapRFS to provide out-of-the-box
 // compatibility with Hadoop cluster. This can be overridden with '-D' switch.
  static {
    props.put("fs.hdfs.impl", // core-default.xml
      FS_MAPRFS_IMPL);
  }
  public static final String FS_WEBHDFS_IMPL = "org.apache.hadoop.hdfs.web.WebHdfsFileSystem";
  static {
    props.put("fs.webhdfs.impl", // core-default.xml
      FS_WEBHDFS_IMPL);
  }
  public static final String FS_MAPR_WORKING_DIR = "/user/$USERNAME/";
  static {
    props.put("fs.mapr.working.dir",
      FS_MAPR_WORKING_DIR);
  }
  public static final String FS_RAMFS_IMPL = "org.apache.hadoop.fs.InMemoryFileSystem";
  static {
    props.put("fs.ramfs.impl", // core-default.xml
      FS_RAMFS_IMPL);
  }
  public static final String FS_S3_BLOCK_SIZE = "33554432";
  static {
    props.put("fs.s3.block.size", // core-default.xml
      FS_S3_BLOCK_SIZE);
  }
  public static final String FS_S3_BLOCKSIZE = "33554432";
  static {
    props.put("fs.s3.blockSize", // core-default.xml
      FS_S3_BLOCKSIZE);
  }
  public static final String FS_S3_BUFFER_DIR = "${hadoop.tmp.dir}/s3";
  static {
    props.put("fs.s3.buffer.dir", // core-default.xml
      FS_S3_BUFFER_DIR);
  }
  public static final String FS_S3N_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
  static {
    props.put("fs.s3n.impl", // core-default.xml
      FS_S3N_IMPL);
  }
  public static final String FS_S3_IMPL = "org.apache.hadoop.fs.s3.S3FileSystem";
  static {
    props.put("fs.s3.impl", // core-default.xml
      FS_S3N_IMPL); // Amazon EMR uses native FileSystem for both s3 and s3n
  }
  public static final String FS_S3_MAXRETRIES = "4";
  static {
    props.put("fs.s3.maxRetries", // core-default.xml
      FS_S3_MAXRETRIES);
  }
  public static final String FS_S3N_BLOCK_SIZE = "33554432";
  static {
    props.put("fs.s3n.block.size", // core-default.xml
      FS_S3N_BLOCK_SIZE);
  }
  public static final String FS_S3N_BLOCKSIZE = "33554432";
  static {
    props.put("fs.s3n.blockSize",
      FS_S3N_BLOCKSIZE);
  }
  public static final String FS_S3_SLEEPTIMESECONDS = "10";
  static {
    props.put("fs.s3.sleepTimeSeconds", // core-default.xml
      FS_S3_SLEEPTIMESECONDS);
  }
  public static final String FS_TRASH_INTERVAL = "0";
  static {
    props.put("fs.trash.interval", // core-default.xml
      FS_TRASH_INTERVAL);
  }
  public static final String HADOOP_LOGFILE_COUNT = "10";
  static {
    props.put("hadoop.logfile.count", // core-default.xml
      HADOOP_LOGFILE_COUNT);
  }
  public static final String HADOOP_LOGFILE_SIZE = "10000000";
  static {
    props.put("hadoop.logfile.size",  // core-default.xml
      HADOOP_LOGFILE_SIZE);
  }
  public static final String HADOOP_NATIVE_LIB = "true";
  static {
    props.put("hadoop.native.lib", // core-default.xml
      HADOOP_NATIVE_LIB);
  }
  public static final String HADOOP_PROXYUSER_ROOT_GROUPS = "root";
  static {
    props.put("hadoop.proxyuser.root.groups", // mapred-default.xml
      HADOOP_PROXYUSER_ROOT_GROUPS);
  }
  public static final String HADOOP_PROXYUSER_ROOT_HOSTS = "*";
  static {
    props.put("hadoop.proxyuser.root.hosts", // mapred-default.xml
      HADOOP_PROXYUSER_ROOT_HOSTS);
  }
  public static final String HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT = "org.apache.hadoop.net.StandardSocketFactory";
  static {
    props.put("hadoop.rpc.socket.factory.class.default", // core-default.xml
      HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT);
  }
  
  public static final String HADOOP_JOB_TICKET_EXPIRATION = "604800000"; // seven days in ms.
  
  static  {
    props.put("mapreduce.job.ticket.expiration.default", // mapred-default.xml
        HADOOP_JOB_TICKET_EXPIRATION);
  }
  public static final String HADOOP_SECURITY_AUTHENTICATION = "simple";
  static {
    if ( isSecurityEnabled ) {
      props.put("hadoop.security.authentication", // core-default.xml
          "maprsasl");      
    } else {
      props.put("hadoop.security.authentication", // core-default.xml
          HADOOP_SECURITY_AUTHENTICATION);
    }
  }
  public static final String HADOOP_SECURITY_AUTHORIZATION = "false";
  static {
    if ( isSecurityEnabled ) {
      props.put("hadoop.security.authorization", // core-default.xml
          "true");    
    } else {
      props.put("hadoop.security.authorization", // core-default.xml
          HADOOP_SECURITY_AUTHORIZATION);
    }
  }
  public static final String HADOOP_SECURITY_PROTECTION = "privacy";
  static {
    if ( isSecurityEnabled ) {
      props.put("hadoop.rpc.protection", // core-site.xml
          HADOOP_SECURITY_PROTECTION);
    }
  }

  public static final String HADOOP_SECURITY_GROUP_MAPPING = "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback";
  static {
    props.put("hadoop.security.group.mapping", // core-default.xml
      HADOOP_SECURITY_GROUP_MAPPING);
  }
  public static final String HADOOP_SECURITY_UID_CACHE_SECS = "14400";
  static {
    props.put("hadoop.security.uid.cache.secs", // core-default.xml
      HADOOP_SECURITY_UID_CACHE_SECS);
  }

  public static final String HADOOP_SSL_KEYSTORE_FACTORY_CLASS = "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory";
  static {
    props.put("hadoop.ssl.keystores.factory.class", // core-default.xml
        HADOOP_SSL_KEYSTORE_FACTORY_CLASS);
  }

  public static final String HADOOP_INSTRUMENTATION_REQUIRES_ADMIN = "false";
  static {
    props.put("hadoop.security.instrumentation.requires.admin", // core-default.xml
        HADOOP_INSTRUMENTATION_REQUIRES_ADMIN);
  }

  public static final String HADOOP_SSL_REQUIRE_CLIENT_CERT = "false";
  static {
    props.put("hadoop.ssl.require.client.cert", // core-default.xml
        HADOOP_SSL_REQUIRE_CLIENT_CERT);
  }

  public static final String SSL_EXCLUDE_CIPHER_SUITES =
          "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA," +
          "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA," +
          "SSL_RSA_EXPORT_WITH_RC4_40_MD5";

  static {
    props.put("hadoop.ssl.exclude.cipher.suites", // core-default.xml
        SSL_EXCLUDE_CIPHER_SUITES);
  }

  public static final String HADOOP_SSL_HOSTNAME_VERIFIER = "DEFAULT";
  static {
    props.put("hadoop.ssl.hostname.verifier", // core-default.xml
        HADOOP_SSL_HOSTNAME_VERIFIER);
  }

  public static final String HADOOP_SSL_SERVER_CONF = "ssl-server.xml";
  static {
    props.put("hadoop.ssl.server.conf", // core-default.xml
        HADOOP_SSL_SERVER_CONF);
  }

  public static final String HADOOP_SSL_CLIENT_CONF = "ssl-client.xml";
  static {
    props.put("hadoop.ssl.client.conf", // core-default.xml
        HADOOP_SSL_CLIENT_CONF);
  }

  public static final String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED = "false";
  static {
    props.put("ipc.client.fallback-to-simple-auth-allowed", // core-default.xml
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED);
  }

  public static final String HADOOP_TMP_DIR = "/tmp/hadoop-${user.name}";
  static {
    props.put("hadoop.tmp.dir", // core-default.xml
      HADOOP_TMP_DIR);
  }
  public static final String HADOOP_UTIL_HASH_TYPE = "murmur";
  static {
    props.put("hadoop.util.hash.type", // core-default.xml
      HADOOP_UTIL_HASH_TYPE);
  }
  public static final String HADOOP_WORKAROUND_NON_THREADSAFE_GETPWUID = "false";
  static {
    props.put("hadoop.workaround.non.threadsafe.getpwuid", // core-default.xml
      HADOOP_WORKAROUND_NON_THREADSAFE_GETPWUID);
  }
  public static final String IO_BYTES_PER_CHECKSUM = "512";
  static {
    props.put("io.bytes.per.checksum", // core-default.xml
      IO_BYTES_PER_CHECKSUM);
  }
  public static final String IO_COMPRESSION_CODECS = "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec";
  static {
    props.put("io.compression.codecs", // core-default.xml
      IO_COMPRESSION_CODECS);
  }
  public static final String IO_FILE_BUFFER_SIZE = "8192";
  static {
    props.put("io.file.buffer.size", // core-default.xml
      IO_FILE_BUFFER_SIZE);
  }
  public static final String IO_MAPFILE_BLOOM_ERROR_RATE = "0.005";
  static {
    props.put("io.mapfile.bloom.error.rate", // core-default.xml
      IO_MAPFILE_BLOOM_ERROR_RATE);
  }
  public static final String IO_MAPFILE_BLOOM_SIZE = "1048576";
  static {
    props.put("io.mapfile.bloom.size", // core-default.xml
      IO_MAPFILE_BLOOM_SIZE);
  }
  public static final String IO_MAP_INDEX_SKIP = "0";
  static {
    props.put("io.map.index.skip", // mapred-default.xml
      IO_MAP_INDEX_SKIP);
  }
  public static final String IO_SEQFILE_COMPRESS_BLOCKSIZE = "1000000";
  static {
    props.put("io.seqfile.compress.blocksize", // core-default.xml
      IO_SEQFILE_COMPRESS_BLOCKSIZE);
  }
  public static final String IO_SEQFILE_LAZYDECOMPRESS = "true";
  static {
    props.put("io.seqfile.lazydecompress", // core-default.xml
      IO_SEQFILE_LAZYDECOMPRESS);
  }
  public static final String IO_SEQFILE_SORTER_RECORDLIMIT = "1000000";
  static {
    props.put("io.seqfile.sorter.recordlimit", // core-default.xml
      IO_SEQFILE_SORTER_RECORDLIMIT);
  }
  public static final String IO_SERIALIZATIONS = "org.apache.hadoop.io.serializer.WritableSerialization";
  static {
    props.put("io.serializations", // core-default.xml
      IO_SERIALIZATIONS);
  }
  public static final String IO_SKIP_CHECKSUM_ERRORS = "false";
  static {
    props.put("io.skip.checksum.errors",  // core-default.xml
      IO_SKIP_CHECKSUM_ERRORS);
  }
  public static final String IO_SORT_FACTOR = "256";
  static {
    props.put("io.sort.factor", // mapred-default.xml
      IO_SORT_FACTOR);
  }
  public static final String IO_SORT_MB = "380";
  static {
    props.put("io.sort.mb", // mapred-default.xml
      IO_SORT_MB);
  }
  public static final String IO_SORT_RECORD_PERCENT = "0.17";
  static {
    props.put("io.sort.record.percent", // mapred-default.xml
      IO_SORT_RECORD_PERCENT);
  }
  public static final String IO_SORT_SPILL_PERCENT = "0.99";
  static {
    props.put("io.sort.spill.percent", // mapred-default.xml
      IO_SORT_SPILL_PERCENT);
  }
  public static final String IPC_CLIENT_CONNECTION_MAXIDLETIME = "10000";
  static {
    props.put("ipc.client.connection.maxidletime", // core-default.xml
      IPC_CLIENT_CONNECTION_MAXIDLETIME);
  }
  public static final String IPC_CLIENT_CONNECT_MAX_RETRIES = "10";
  static {
    props.put("ipc.client.connect.max.retries", // core-default.xml
      IPC_CLIENT_CONNECT_MAX_RETRIES);
  }
  public static final String IPC_CLIENT_IDLETHRESHOLD = "4000";
  static {
    props.put("ipc.client.idlethreshold", // core-default.xml
      IPC_CLIENT_IDLETHRESHOLD);
  }
  public static final String IPC_CLIENT_KILL_MAX = "10";
  static {
    props.put("ipc.client.kill.max", // core-default.xml
      IPC_CLIENT_KILL_MAX);
  }
  public static final String IPC_CLIENT_MAX_CONNECTION_SETUP_TIMEOUT = "20";
  static {
    props.put("ipc.client.max.connection.setup.timeout", // core-default.xml
      IPC_CLIENT_MAX_CONNECTION_SETUP_TIMEOUT);
  }
  public static final String IPC_CLIENT_TCPNODELAY = "true";
  static {
    props.put("ipc.client.tcpnodelay", // core-default.xml
      IPC_CLIENT_TCPNODELAY);
  }
  public static final String IPC_SERVER_LISTEN_QUEUE_SIZE = "128";
  static {
    props.put("ipc.server.listen.queue.size", // core-default.xml
      IPC_SERVER_LISTEN_QUEUE_SIZE);
  }
  public static final String IPC_SERVER_TCPNODELAY = "true";
  static {
    props.put("ipc.server.tcpnodelay", // core-default.xml
      IPC_SERVER_TCPNODELAY);
  }

  public static final String JOBCLIENT_COMPLETION_POLL_INTERVAL = "5000";
  static {
    props.put("jobclient.completion.poll.interval", // mapred-default.xml
      JOBCLIENT_COMPLETION_POLL_INTERVAL);
  }
  public static final String JOBCLIENT_OUTPUT_FILTER = "FAILED";
  static {
    props.put("jobclient.output.filter", // mapred-default.xml
      JOBCLIENT_OUTPUT_FILTER);
  }
  public static final String JOBCLIENT_PROGRESS_MONITOR_POLL_INTERVAL = "1000";
  static {
    props.put("jobclient.progress.monitor.poll.interval", // mapred-default.xml
      JOBCLIENT_PROGRESS_MONITOR_POLL_INTERVAL);
  }
  public static final String JOB_END_RETRY_ATTEMPTS = "0";
  static {
    props.put("job.end.retry.attempts", // mapred-default.xml
      JOB_END_RETRY_ATTEMPTS);
  }
  public static final String JOB_END_RETRY_INTERVAL = "30000";
  static {
    props.put("job.end.retry.interval", // mapred-default.xml
      JOB_END_RETRY_INTERVAL);
  }
  public static final String KEEP_FAILED_TASK_FILES = "false";
  static {
    props.put("keep.failed.task.files", // mapred-default.xml
      KEEP_FAILED_TASK_FILES);
  }
  public static final String LOCAL_CACHE_SIZE = "10737418240";
  static {
    props.put("local.cache.size",
      LOCAL_CACHE_SIZE);
  }
  public static final String MAPR_TASK_DIAGNOSTICS_ENABLED = "false";
  static {
    props.put("mapr.task.diagnostics.enabled", MAPR_TASK_DIAGNOSTICS_ENABLED);
  }
  public static final String MAPR_CENTRALLOG_DIR = "logs";
  static {
    props.put("mapr.centrallog.dir", // mapred-default.xml
      MAPR_CENTRALLOG_DIR);
  }
  public static final String MAPRED_ACLS_ENABLED = "false";
  static {
    if ( isSecurityEnabled ) {
      props.put("mapred.acls.enabled", // mapred-default.xml
          "true");      
    } else {
      props.put("mapred.acls.enabled", // mapred-default.xml
          MAPRED_ACLS_ENABLED);
    }
  }
  public static final String MAPRED_CHILD_OOM_ADJ = "10";
  static {
    props.put("mapred.child.oom_adj", // mapred-default.xml
      MAPRED_CHILD_OOM_ADJ);
  }
  public static final String MAPRED_CHILD_RENICE = "10";
  static {
    props.put("mapred.child.renice", // mapred-default.xml
      MAPRED_CHILD_RENICE);
  }
  public static final String MAPRED_CHILD_TASKSET = "true";
  static {
    props.put("mapred.child.taskset", // mapred-default.xml
      MAPRED_CHILD_TASKSET);
  }
  public static final String MAPRED_CHILD_TMP = "./tmp";
  static {
    props.put("mapred.child.tmp", // mapred-default.xml
      MAPRED_CHILD_TMP);
  }
  public static final String MAPRED_CLUSTER_EPHEMERAL_TASKS_MEMORY_LIMIT_MB = "200";
  static {
    props.put("mapred.cluster.ephemeral.tasks.memory.limit.mb", // mapred-site.xml
      MAPRED_CLUSTER_EPHEMERAL_TASKS_MEMORY_LIMIT_MB);
  }
  public static final String MAPRED_CLUSTER_MAP_MEMORY_MB = "-1";
  static {
    props.put("mapred.cluster.map.memory.mb", // mapred-default.xml
      MAPRED_CLUSTER_MAP_MEMORY_MB);
  }
  public static final String MAPRED_CLUSTER_MAX_MAP_MEMORY_MB = "-1";
  static {
    props.put("mapred.cluster.max.map.memory.mb", // mapred-default.xml
      MAPRED_CLUSTER_MAX_MAP_MEMORY_MB);
  }
  public static final String MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB = "-1";
  static {
    props.put("mapred.cluster.max.reduce.memory.mb", // mapred-default.xml
      MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB);
  }
  public static final String MAPRED_CLUSTER_REDUCE_MEMORY_MB = "-1";
  static {
    props.put("mapred.cluster.reduce.memory.mb", // mapred-default.xml
      MAPRED_CLUSTER_REDUCE_MEMORY_MB);
  }
  public static final String MAPRED_COMBINE_RECORDSBEFOREPROGRESS = "10000";
  static {
    props.put("mapred.combine.recordsBeforeProgress", // mapred-default.xml
      MAPRED_COMBINE_RECORDSBEFOREPROGRESS);
  }
  public static final String MAPRED_COMPRESS_MAP_OUTPUT = "false";
  static {
    props.put("mapred.compress.map.output", // mapred-default.xml
      MAPRED_COMPRESS_MAP_OUTPUT);
  }
  public static final String MAPRED_FAIRSCHEDULER_ASSIGNMULTIPLE = "true";
  static {
    props.put("mapred.fairscheduler.assignmultiple", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_ASSIGNMULTIPLE);
  }
  public static final String MAPRED_FAIRSCHEDULER_EVENTLOG_ENABLED = "false";
  static {
    props.put("mapred.fairscheduler.eventlog.enabled", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_EVENTLOG_ENABLED);
  }
  public static final String MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_INPUTSIZE = "10737418240";
  static {
    props.put("mapred.fairscheduler.smalljob.max.inputsize", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_INPUTSIZE);
  }
  public static final String MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_MAPS = "10";
  static {
    props.put("mapred.fairscheduler.smalljob.max.maps", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_MAPS);
  }
  public static final String MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_REDUCER_INPUTSIZE = "1073741824";
  static {
    props.put("mapred.fairscheduler.smalljob.max.reducer.inputsize",  // mapred-site.xml
      MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_REDUCER_INPUTSIZE);
  }
  public static final String MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_REDUCERS = "10";
  static {
    props.put("mapred.fairscheduler.smalljob.max.reducers", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_SMALLJOB_MAX_REDUCERS);
  }
  public static final String MAPRED_FAIRSCHEDULER_SMALLJOB_SCHEDULE_ENABLE = "true";
  static {
    props.put("mapred.fairscheduler.smalljob.schedule.enable", // mapred-site.xml
      MAPRED_FAIRSCHEDULER_SMALLJOB_SCHEDULE_ENABLE);
  }
  public static final String MAPRED_HEALTHCHECKER_INTERVAL = "60000";
  static {
    props.put("mapred.healthChecker.interval", // mapred-default.xml
      MAPRED_HEALTHCHECKER_INTERVAL);
  }
  public static final String MAPRED_HEALTHCHECKER_SCRIPT_TIMEOUT = "600000";
  static {
    props.put("mapred.healthChecker.script.timeout", // mapred-default.xml
      MAPRED_HEALTHCHECKER_SCRIPT_TIMEOUT);
  }
  public static final String MAPRED_INMEM_MERGE_THRESHOLD = "1000";
  static {
    props.put("mapred.inmem.merge.threshold", // mapred-default.xml
      MAPRED_INMEM_MERGE_THRESHOLD);
  }
  public static final String MAPRED_JOB_MAP_MEMORY_MB = "-1";
  static {
    props.put("mapred.job.map.memory.mb", // mapred-default.xml
      MAPRED_JOB_MAP_MEMORY_MB);
  }
  public static final String MAPRED_JOB_QUEUE_NAME = "default";
  static {
    props.put("mapred.job.queue.name", // mapred-default.xml
      MAPRED_JOB_QUEUE_NAME);
  }
  public static final String MAPRED_JOB_REDUCE_INPUT_BUFFER_PERCENT = "0.0";
  static {
    props.put("mapred.job.reduce.input.buffer.percent", // mapred-default.xml
      MAPRED_JOB_REDUCE_INPUT_BUFFER_PERCENT);
  }
  public static final String MAPRED_JOB_REDUCE_MEMORY_MB = "-1";
  static {
    props.put("mapred.job.reduce.memory.mb", // mapred-default.xml
      MAPRED_JOB_REDUCE_MEMORY_MB);
  }
  public static final String MAPRED_JOB_REUSE_JVM_NUM_TASKS = "-1";
  static {
    props.put("mapred.job.reuse.jvm.num.tasks", // mapred-default.xml
      MAPRED_JOB_REUSE_JVM_NUM_TASKS);
  }
  public static final String MAPRED_JOB_SHUFFLE_INPUT_BUFFER_PERCENT = "0.70";
  static {
    props.put("mapred.job.shuffle.input.buffer.percent", // mapred-default.xml
      MAPRED_JOB_SHUFFLE_INPUT_BUFFER_PERCENT);
  }
  public static final String MAPRED_JOB_SHUFFLE_MERGE_PERCENT = "0.66";
  static {
    props.put("mapred.job.shuffle.merge.percent", // mapred-default.xml
      MAPRED_JOB_SHUFFLE_MERGE_PERCENT);
  }
  public static final String MAPRED_JOB_TRACKER_HANDLER_COUNT = "10";
  static {
    props.put("mapred.job.tracker.handler.count", // mapred-default.xml
      MAPRED_JOB_TRACKER_HANDLER_COUNT);
  }
  public static final String MAPRED_JOB_TRACKER_HISTORY_COMPLETED_LOCATION = "/var/mapr/cluster/mapred/jobTracker/history/done";
  static {
    props.put("mapred.job.tracker.history.completed.location", // mapred-default.xml
      MAPRED_JOB_TRACKER_HISTORY_COMPLETED_LOCATION);
  }
  public static final String MAPRED_JOB_TRACKER_HTTP_ADDRESS = "0.0.0.0:50030";
  static {
    props.put("mapred.job.tracker.http.address", // mapred-default.xml
      MAPRED_JOB_TRACKER_HTTP_ADDRESS);
  }
  public static final String MAPRED_JOBTRACKER_INSTRUMENTATION = "org.apache.hadoop.mapred.JobTrackerMetricsInst";
  static {
    props.put("mapred.jobtracker.instrumentation", // mapred-default.xml
      MAPRED_JOBTRACKER_INSTRUMENTATION);
  }
  public static final String MAPRED_JOBTRACKER_JOB_HISTORY_BLOCK_SIZE = "3145728";
  static {
    props.put("mapred.jobtracker.job.history.block.size", // mapred-default.xml
      MAPRED_JOBTRACKER_JOB_HISTORY_BLOCK_SIZE);
  }
  public static final String MAPRED_JOBTRACKER_JOBHISTORY_LRU_CACHE_SIZE = "5";
  static {
    props.put("mapred.jobtracker.jobhistory.lru.cache.size", // mapred-default.xml
      MAPRED_JOBTRACKER_JOBHISTORY_LRU_CACHE_SIZE);
  }
  public static final String MAPRED_JOBTRACKER_MAXTASKS_PER_JOB = "-1";
  static {
    props.put("mapred.jobtracker.maxtasks.per.job", // mapred-default.xml
      MAPRED_JOBTRACKER_MAXTASKS_PER_JOB);
  }
  public static final String MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_ACTIVE = "false";
  static {
    props.put("mapred.job.tracker.persist.jobstatus.active", // mapred-default.xml
      MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_ACTIVE);
  }
  public static final String MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_DIR = "/var/mapr/cluster/mapred/jobTracker/jobsInfo";
  static {
    props.put("mapred.job.tracker.persist.jobstatus.dir", // mapred-default.xml
      MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_DIR);
  }
  public static final String MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_HOURS = "0";
  static {
    props.put("mapred.job.tracker.persist.jobstatus.hours", // mapred-default.xml
      MAPRED_JOB_TRACKER_PERSIST_JOBSTATUS_HOURS);
  }
  public static final String MAPRED_JOBTRACKER_PORT = "9001";
  static {
    props.put("mapred.jobtracker.port", // mapred-default.xml
      MAPRED_JOBTRACKER_PORT);
  }
  public static final String MAPRED_JOBTRACKER_RESTART_RECOVER = "true";
  static {
    props.put("mapred.jobtracker.restart.recover", // mapred-default.xml
      MAPRED_JOBTRACKER_RESTART_RECOVER);
  }
  public static final String MAPRED_JOBTRACKER_RETIREDJOBS_CACHE_SIZE = "1000";
  static {
    props.put("mapred.jobtracker.retiredjobs.cache.size", // mapred-default.xml
      MAPRED_JOBTRACKER_RETIREDJOBS_CACHE_SIZE);
  }
  public static final String MAPRED_JOBTRACKER_RETIREJOB_CHECK = "30000";
  static {
    props.put("mapred.jobtracker.retirejob.check", // mapred-default.xml
      MAPRED_JOBTRACKER_RETIREJOB_CHECK);
  }
  public static final String MAPRED_JOBTRACKER_TASKSCHEDULER = "org.apache.hadoop.mapred.FairScheduler";
  static {
    props.put("mapred.jobtracker.taskScheduler", // mapred-default.xml
      MAPRED_JOBTRACKER_TASKSCHEDULER);
  }
  public static final String MAPRED_JOB_TRACKER = "maprfs:///";
  static {
    props.put("mapred.job.tracker", // mapred-default.xml
      MAPRED_JOB_TRACKER);
  }
  public static final String MAPRED_LINE_INPUT_FORMAT_LINESPERMAP = "1";
  static {
    props.put("mapred.line.input.format.linespermap", // mapred-default.xml
      MAPRED_LINE_INPUT_FORMAT_LINESPERMAP);
  }
  public static final String MAPRED_LOCAL_DIR_MINSPACEKILL = "0";
  static {
    props.put("mapred.local.dir.minspacekill", // mapred-default.xml
      MAPRED_LOCAL_DIR_MINSPACEKILL);
  }
  public static final String MAPRED_LOCAL_DIR_MINSPACESTART = "0";
  static {
    props.put("mapred.local.dir.minspacestart", // mapred-default.xml
      MAPRED_LOCAL_DIR_MINSPACESTART);
  }
  public static final String MAPRED_LOCAL_DIR = "/tmp/mapr-hadoop/mapred/local";
  static {
    props.put("mapred.local.dir", // mapred-default.xml
      MAPRED_LOCAL_DIR);
  }
  public static final String MAPRED_MAP_CHILD_JAVA_OPTS = "-XX:ErrorFile=/opt/cores/mapreduce_java_error%p.log";
  static {
    props.put("mapred.map.child.java.opts", // mapred-default.xml
      MAPRED_MAP_CHILD_JAVA_OPTS);
  }
  public static final String MAPRED_MAP_CHILD_LOG_LEVEL = "INFO";
  static {
    props.put("mapred.map.child.log.level", // mapred-default.xml
      MAPRED_MAP_CHILD_LOG_LEVEL);
  }
  public static final String MAPRED_MAP_MAX_ATTEMPTS = "4";
  static {
    props.put("mapred.map.max.attempts", // mapred-default.xml
      MAPRED_MAP_MAX_ATTEMPTS);
  }
  public static final String MAPRED_MAP_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.DefaultCodec";
  static {
    props.put("mapred.map.output.compression.codec", // mapred-default.xml
      MAPRED_MAP_OUTPUT_COMPRESSION_CODEC);
  }
  public static final String MAPRED_MAPTASK_MEMORY_MB_DEFAULT = "1024";
  static {
    props.put("mapred.maptask.memory.default", // mapred-default.xml
      MAPRED_MAPTASK_MEMORY_MB_DEFAULT);
  }
  public static final String MAPRED_MAP_TASKS = "2";
  static {
    props.put("mapred.map.tasks", // mapred-default.xml
      MAPRED_MAP_TASKS);
  }
  public static final String MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION = "true";
  static {
    props.put("mapred.map.tasks.speculative.execution", // mapred-default.xml
      MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION);
  }
  public static final String MAPRED_MAXTHREADS_GENERATE_MAPOUTPUT = "1";
  static {
    props.put("mapred.maxthreads.generate.mapoutput", // mapred-default.xml
      MAPRED_MAXTHREADS_GENERATE_MAPOUTPUT);
  }
  public static final String MAPRED_MAXTHREADS_PARTITION_CLOSER = "1";
  static {
    props.put("mapred.maxthreads.partition.closer", // mapred-default.xml
      MAPRED_MAXTHREADS_PARTITION_CLOSER);
  }

  public static final String MAPRED_MAX_TRACKER_BLACKLISTS = "4";
  static {
    props.put("mapred.max.tracker.blacklists", // mapred-default.xml
      MAPRED_MAX_TRACKER_BLACKLISTS);
  }
  public static final String MAPRED_MAX_TRACKER_FAILURES = "4";
  static {
    props.put("mapred.max.tracker.failures", // mapred-default.xml
      MAPRED_MAX_TRACKER_FAILURES);
  }
  public static final String MAPRED_MERGE_RECORDSBEFOREPROGRESS = "10000";
  static {
    props.put("mapred.merge.recordsBeforeProgress", // mapred-default.xml
      MAPRED_MERGE_RECORDSBEFOREPROGRESS);
  }
  public static final String MAPRED_MIN_SPLIT_SIZE = "0";
  static {
    props.put("mapred.min.split.size", // mapred-default.xml
      MAPRED_MIN_SPLIT_SIZE);
  }
  public static final String MAPRED_OUTPUT_COMPRESS = "false";
  static {
    props.put("mapred.output.compress", // mapred-default.xml
      MAPRED_OUTPUT_COMPRESS);
  }
  public static final String MAPRED_OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.DefaultCodec";
  static {
    props.put("mapred.output.compression.codec", // mapred-default.xml
      MAPRED_OUTPUT_COMPRESSION_CODEC);
  }
  public static final String MAPRED_OUTPUT_COMPRESSION_TYPE = "RECORD";
  static {
    props.put("mapred.output.compression.type", // mapred-default.xml
      MAPRED_OUTPUT_COMPRESSION_TYPE);
  }
  public static final String MAPRED_QUEUE_DEFAULT_STATE = "RUNNING";
  static {
    props.put("mapred.queue.default.state", // mapred-default.xml
      MAPRED_QUEUE_DEFAULT_STATE);
  }
  public static final String MAPRED_QUEUE_NAMES = "default";
  static {
    props.put("mapred.queue.names", // mapred-default.xml
      MAPRED_QUEUE_NAMES);
  }
  public static final String MAPRED_REDUCE_CHILD_JAVA_OPTS = "-XX:ErrorFile=/opt/cores/mapreduce_java_error%p.log";
  static {
    props.put("mapred.reduce.child.java.opts", // mapred-default.xml
      MAPRED_REDUCE_CHILD_JAVA_OPTS);
  }
  public static final String MAPRED_REDUCE_CHILD_LOG_LEVEL = "INFO";
  static {
    props.put("mapred.reduce.child.log.level", // mapred-default.xml
      MAPRED_REDUCE_CHILD_LOG_LEVEL);
  }
  public static final String MAPRED_REDUCE_COPY_BACKOFF = "300";
  static {
    props.put("mapred.reduce.copy.backoff", // mapred-default.xml
      MAPRED_REDUCE_COPY_BACKOFF);
  }
  public static final String MAPRED_REDUCE_MAX_ATTEMPTS = "4";
  static {
    props.put("mapred.reduce.max.attempts", // mapred-default.xml
      MAPRED_REDUCE_MAX_ATTEMPTS);
  }
  public static final String MAPRED_REDUCE_PARALLEL_COPIES = "12";
  static {
    props.put("mapred.reduce.parallel.copies", // mapred-default.xml
      MAPRED_REDUCE_PARALLEL_COPIES);
  }
  public static final String MAPRED_REDUCE_SLOWSTART_COMPLETED_MAPS = "0.95";
  static {
    props.put("mapred.reduce.slowstart.completed.maps", // mapred-default.xml
      MAPRED_REDUCE_SLOWSTART_COMPLETED_MAPS);
  }
  public static final String MAPRED_REDUCETASK_MEMORY_MB_DEFAULT = "3072";
  static {
    props.put("mapred.reducetask.memory.default", // mapred-default.xml
      MAPRED_REDUCETASK_MEMORY_MB_DEFAULT);
  }
  public static final String MAPRED_REDUCE_TASKS = "1";
  static {
    props.put("mapred.reduce.tasks", // mapred-default.xml
      MAPRED_REDUCE_TASKS);
  }
  public static final String MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION = "true";
  static {
    props.put("mapred.reduce.tasks.speculative.execution", // mapred-default.xml
      MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION);
  }
  public static final String MAPRED_SKIP_ATTEMPTS_TO_START_SKIPPING = "2";
  static {
    props.put("mapred.skip.attempts.to.start.skipping", // mapred-default.xml
      MAPRED_SKIP_ATTEMPTS_TO_START_SKIPPING);
  }
  public static final String MAPRED_SKIP_MAP_AUTO_INCR_PROC_COUNT = "true";
  static {
    props.put("mapred.skip.map.auto.incr.proc.count", // mapred-default.xml
      MAPRED_SKIP_MAP_AUTO_INCR_PROC_COUNT);
  }
  public static final String MAPRED_SKIP_MAP_MAX_SKIP_RECORDS = "0";
  static {
    props.put("mapred.skip.map.max.skip.records", // mapred-default.xml
      MAPRED_SKIP_MAP_MAX_SKIP_RECORDS);
  }
  public static final String MAPRED_SKIP_REDUCE_AUTO_INCR_PROC_COUNT = "true";
  static {
    props.put("mapred.skip.reduce.auto.incr.proc.count", // mapred-default.xml
      MAPRED_SKIP_REDUCE_AUTO_INCR_PROC_COUNT);
  }
  public static final String MAPRED_SKIP_REDUCE_MAX_SKIP_GROUPS = "0";
  static {
    props.put("mapred.skip.reduce.max.skip.groups", // mapred-default.xml
      MAPRED_SKIP_REDUCE_MAX_SKIP_GROUPS);
  }
  public static final String MAPRED_SUBMIT_REPLICATION = "10";
  static {
    props.put("mapred.submit.replication", // mapred-default.xml
      MAPRED_SUBMIT_REPLICATION);
  }
  public static final String MAPRED_SYSTEM_DIR = "/var/mapr/cluster/mapred/jobTracker/system";
  static {
    props.put("mapred.system.dir", // mapred-default.xml
      MAPRED_SYSTEM_DIR);
  }
  public static final String MAPRED_TASK_CACHE_LEVELS = "2";
  static {
    props.put("mapred.task.cache.levels", // mapred-default.xml
      MAPRED_TASK_CACHE_LEVELS);
  }
  public static final String MAPRED_TASK_CALCULATE_RESOURCE_USAGE = "true";
  static {
    props.put("mapred.task.calculate.resource.usage", // mapred-default.xml
      MAPRED_TASK_CALCULATE_RESOURCE_USAGE);
  }
  public static final String MAPRED_TASK_PING_TIMEOUT = "60000";
  static {
    props.put("mapred.task.ping.timeout", // mapred-default.xml
      MAPRED_TASK_PING_TIMEOUT);
  }
  public static final String MAPRED_TASK_PROFILE = "false";
  static {
    props.put("mapred.task.profile", // mapred-default.xml
      MAPRED_TASK_PROFILE);
  }
  public static final String MAPRED_TASK_PROFILE_MAPS = "0-2";
  static {
    props.put("mapred.task.profile.maps", // mapred-default.xml
      MAPRED_TASK_PROFILE_MAPS);
  }
  public static final String MAPRED_TASK_PROFILE_REDUCES = "0-2";
  static {
    props.put("mapred.task.profile.reduces", // mapred-default.xml
      MAPRED_TASK_PROFILE_REDUCES);
  }
  public static final String MAPRED_TASK_PROGRESS_INTERVAL = "3000";
  static {
    props.put("mapred.task.progress.interval", // mapred-default.xml
      MAPRED_TASK_PROGRESS_INTERVAL);
  }
  public static final String MAPRED_TASK_TIMEOUT = "600000";
  static {
    props.put("mapred.task.timeout", // mapred-default.xml
      MAPRED_TASK_TIMEOUT);
  }
  public static final String MAPRED_TASKTRACKER_DNS_INTERFACE = "default";
  static {
    props.put("mapred.tasktracker.dns.interface", // mapred-default.xml
      MAPRED_TASKTRACKER_DNS_INTERFACE);
  }
  public static final String MAPRED_TASKTRACKER_DNS_NAMESERVER = "default";
  static {
    props.put("mapred.tasktracker.dns.nameserver", // mapred-default.xml
      MAPRED_TASKTRACKER_DNS_NAMESERVER);
  }
  public static final String MAPRED_TASKTRACKER_EPHEMERAL_TASKS_MAXIMUM = "1";
  static {
    props.put("mapred.tasktracker.ephemeral.tasks.maximum", // mapred-site.xml
      MAPRED_TASKTRACKER_EPHEMERAL_TASKS_MAXIMUM);
  }
  public static final String MAPRED_TASKTRACKER_EPHEMERAL_TASKS_TIMEOUT = "10000";
  static {
    props.put("mapred.tasktracker.ephemeral.tasks.timeout", // mapred-site.xml
      MAPRED_TASKTRACKER_EPHEMERAL_TASKS_TIMEOUT);
  }
  public static final String MAPRED_TASKTRACKER_EPHEMERAL_TASKS_ULIMIT = "4294967296";
  static {
    props.put("mapred.tasktracker.ephemeral.tasks.ulimit", // mapred-site.xml
      MAPRED_TASKTRACKER_EPHEMERAL_TASKS_ULIMIT);
  }
  public static final String MAPRED_TASKTRACKER_EXPIRY_INTERVAL = "600000";
  static {
    props.put("mapred.tasktracker.expiry.interval", // mapred-default.xml
      MAPRED_TASKTRACKER_EXPIRY_INTERVAL);
  }
  public static final String MAPRED_TASK_TRACKER_HTTP_ADDRESS = "0.0.0.0:50060";
  static {
    props.put("mapred.task.tracker.http.address", // mapred-default.xml
      MAPRED_TASK_TRACKER_HTTP_ADDRESS);
  }
  public static final String MAPRED_TASKTRACKER_INDEXCACHE_MB = "10";
  static {
    props.put("mapred.tasktracker.indexcache.mb", // mapred-default.xml
      MAPRED_TASKTRACKER_INDEXCACHE_MB);
  }
  public static final String MAPRED_TASKTRACKER_INSTRUMENTATION = "org.apache.hadoop.mapred.TaskTrackerMetricsInst";
  static {
    props.put("mapred.tasktracker.instrumentation", // mapred-default.xml
      MAPRED_TASKTRACKER_INSTRUMENTATION);
  }
  public static final String MAPRED_TASKTRACKER_MAP_TASKS_MAXIMUM = "-1";
  static {
    props.put("mapred.tasktracker.map.tasks.maximum", // mapred-default.xml
      MAPRED_TASKTRACKER_MAP_TASKS_MAXIMUM);
  }
  public static final String MAPRED_TASKTRACKER_REDUCE_TASKS_MAXIMUM = "-1";
  static {
    props.put("mapred.tasktracker.reduce.tasks.maximum", // mapred-default.xml
      MAPRED_TASKTRACKER_REDUCE_TASKS_MAXIMUM);
  }
  public static final String MAPRED_TASK_TRACKER_REPORT_ADDRESS = "127.0.0.1:0";
  static {
    props.put("mapred.task.tracker.report.address", // mapred-default.xml
      MAPRED_TASK_TRACKER_REPORT_ADDRESS);
  }
  public static final String MAPRED_TASKTRACKER_TASK_CONTROLLER_CONFIG_OVERWRITE = "true";
  static {
    props.put("mapred.tasktracker.task-controller.config.overwrite", // mapred-default.xml
      MAPRED_TASKTRACKER_TASK_CONTROLLER_CONFIG_OVERWRITE);
  }
  public static final String MAPRED_TASK_TRACKER_TASK_CONTROLLER = "org.apache.hadoop.mapred.LinuxTaskController";
  static {
    props.put("mapred.task.tracker.task-controller", // mapred-default.xml
      MAPRED_TASK_TRACKER_TASK_CONTROLLER);
  }
  public static final String MAPRED_TASKTRACKER_TASKMEMORYMANAGER_KILLTASK_MAXRSS = "false";
  static {
    props.put("mapred.tasktracker.taskmemorymanager.killtask.maxRSS", // mapred-default.xml
      MAPRED_TASKTRACKER_TASKMEMORYMANAGER_KILLTASK_MAXRSS);
  }
  public static final String MAPRED_TASKTRACKER_TASKMEMORYMANAGER_MONITORING_INTERVAL = "3000";
  static {
    props.put("mapred.tasktracker.taskmemorymanager.monitoring-interval", // mapred-default.xml
      MAPRED_TASKTRACKER_TASKMEMORYMANAGER_MONITORING_INTERVAL);
  }
  public static final String MAPRED_TASKTRACKER_TASKS_SLEEPTIME_BEFORE_SIGKILL = "5000";
  static {
    props.put("mapred.tasktracker.tasks.sleeptime-before-sigkill", // mapred-default.xml
      MAPRED_TASKTRACKER_TASKS_SLEEPTIME_BEFORE_SIGKILL);
  }
  public static final String MAPRED_TEMP_DIR = "${hadoop.tmp.dir}/mapred/temp";
  static {
    props.put("mapred.temp.dir", // mapred-default.xml
      MAPRED_TEMP_DIR);
  }
  public static final String MAPREDUCE_CLUSTER_MAP_USERLOG_RETAIN_SIZE = "-1";
  static {
    props.put("mapreduce.cluster.map.userlog.retain-size", // mapred-default.xml
      MAPREDUCE_CLUSTER_MAP_USERLOG_RETAIN_SIZE);
  }
  public static final String MAPREDUCE_CLUSTER_REDUCE_USERLOG_RETAIN_SIZE = "-1";
  static {
    props.put("mapreduce.cluster.reduce.userlog.retain-size", // mapred-default.xml
      MAPREDUCE_CLUSTER_REDUCE_USERLOG_RETAIN_SIZE);
  }
  public static final String MAPREDUCE_HEARTBEAT_10000 = "100000";
  static {
    props.put("mapreduce.heartbeat.10000", // mapred-default.xml
      MAPREDUCE_HEARTBEAT_10000);
  }
  public static final String MAPREDUCE_HEARTBEAT_1000 = "10000";
  static {
    props.put("mapreduce.heartbeat.1000", // mapred-default.xml
      MAPREDUCE_HEARTBEAT_1000);
  }
  public static final String MAPREDUCE_HEARTBEAT_100 = "1000";
  static {
    props.put("mapreduce.heartbeat.100", // mapred-default.xml
      MAPREDUCE_HEARTBEAT_100);
  }
  public static final String MAPREDUCE_HEARTBEAT_10 = "300";
  static {
    props.put("mapreduce.heartbeat.10", // mapred-default.xml
      MAPREDUCE_HEARTBEAT_10);
  }
  public static final String MAPREDUCE_JOB_COMPLETE_CANCEL_DELEGATION_TOKENS = "true";
  static {
    props.put("mapreduce.job.complete.cancel.delegation.tokens", // mapred-default.xml
      MAPREDUCE_JOB_COMPLETE_CANCEL_DELEGATION_TOKENS);
  }
  public static final String MAPREDUCE_JOBTRACKER_INLINE_SETUP_CLEANUP = "false";
  static {
    props.put("mapreduce.jobtracker.inline.setup.cleanup", // mapred-default.xml
      MAPREDUCE_JOBTRACKER_INLINE_SETUP_CLEANUP);
  }
  public static final String MAPREDUCE_JOBTRACKER_NODE_LABELS_MONITOR_INTERVAL = "120000";
  static {
    props.put("mapreduce.jobtracker.node.labels.monitor.interval", // mapred-default.xml
      MAPREDUCE_JOBTRACKER_NODE_LABELS_MONITOR_INTERVAL);
  }
  public static final String MAPREDUCE_JOBTRACKER_RECOVERY_DIR = "/var/mapr/cluster/mapred/jobTracker/recovery";
  static {
    props.put("mapreduce.jobtracker.recovery.dir", // mapred-default.xml
      MAPREDUCE_JOBTRACKER_RECOVERY_DIR);
  }
  public static final String MAPREDUCE_JOBTRACKER_RECOVERY_JOB_INITIALIZATION_MAXTIME = "480";
  static {
    props.put("mapreduce.jobtracker.recovery.job.initialization.maxtime", // mapred-site.xml
      MAPREDUCE_JOBTRACKER_RECOVERY_JOB_INITIALIZATION_MAXTIME);
  }
  public static final String MAPREDUCE_JOBTRACKER_RECOVERY_MAXTIME = "480";
  static {
    props.put("mapreduce.jobtracker.recovery.maxtime", // mapred-default.xml
      MAPREDUCE_JOBTRACKER_RECOVERY_MAXTIME);
  }
  public static final String MAPREDUCE_JOBTRACKER_SPLIT_METAINFO_MAXSIZE = "10000000";
  static {
    props.put("mapreduce.jobtracker.split.metainfo.maxsize", // mapred-default.xml
      MAPREDUCE_JOBTRACKER_SPLIT_METAINFO_MAXSIZE);
  }
  public static final String MAPREDUCE_JOBTRACKER_STAGING_ROOT_DIR = "/var/mapr/cluster/mapred/jobTracker/staging";
  static {
    props.put("mapreduce.jobtracker.staging.root.dir",
      MAPREDUCE_JOBTRACKER_STAGING_ROOT_DIR);
  }
  public static final String MAPREDUCE_MAPRFS_USE_COMPRESSION = "true";
  static {
    props.put("mapreduce.maprfs.use.compression", // mapred-default.xml
      MAPREDUCE_MAPRFS_USE_COMPRESSION);
  }
  public static final String MAPREDUCE_REDUCE_INPUT_LIMIT = "-1";
  static {
    props.put("mapreduce.reduce.input.limit", // mapred-default.xml
      MAPREDUCE_REDUCE_INPUT_LIMIT);
  }
  public static final String MAPREDUCE_TASK_CLASSPATH_USER_PRECEDENCE = "false";
  static {
    props.put("mapreduce.task.classpath.user.precedence", // mapred-default.xml
      MAPREDUCE_TASK_CLASSPATH_USER_PRECEDENCE);
  }
  public static final String MAPREDUCE_TASKTRACKER_CACHE_LOCAL_NUMBERDIRECTORIES = "10000";
  static {
    props.put("mapreduce.tasktracker.cache.local.numberdirectories", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_CACHE_LOCAL_NUMBERDIRECTORIES);
  }
  public static final String MAPREDUCE_TASKTRACKER_GROUP = "mapr";
  static {
    props.put("mapreduce.tasktracker.group", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_GROUP);
  }
  public static final String MAPREDUCE_TASKTRACKER_HEAPBASED_MEMORY_MANAGEMENT = "false";
  static {
    props.put("mapreduce.tasktracker.heapbased.memory.management", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_HEAPBASED_MEMORY_MANAGEMENT);
  }
  public static final String MAPREDUCE_TASKTRACKER_JVM_IDLE_TIME = "10000";
  static {
    props.put("mapreduce.tasktracker.jvm.idle.time", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_JVM_IDLE_TIME);
  }
  public static final String MAPREDUCE_TASKTRACKER_OUTOFBAND_HEARTBEAT = "true";
  static {
    props.put("mapreduce.tasktracker.outofband.heartbeat", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_OUTOFBAND_HEARTBEAT);
  }
  public static final String MAPREDUCE_TASKTRACKER_PREFETCH_MAPTASKS = "0.0";
  static {
    props.put("mapreduce.tasktracker.prefetch.maptasks", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_PREFETCH_MAPTASKS);
  }
  public static final String MAPREDUCE_TASKTRACKER_RESERVED_PHYSICALMEMORY_MB_LOW = "0.80";
  static {
    props.put("mapreduce.tasktracker.reserved.physicalmemory.mb.low", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_RESERVED_PHYSICALMEMORY_MB_LOW);
  }
  public static final String MAPREDUCE_TASKTRACKER_TASK_SLOWLAUNCH = "false";
  static {
    props.put("mapreduce.tasktracker.task.slowlaunch", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_TASK_SLOWLAUNCH);
  }
  public static final String MAPREDUCE_TASKTRACKER_VOLUME_HEALTHCHECK_INTERVAL = "60000";
  static {
    props.put("mapreduce.tasktracker.volume.healthcheck.interval", // mapred-default.xml
      MAPREDUCE_TASKTRACKER_VOLUME_HEALTHCHECK_INTERVAL);
  }
  public static final String MAPREDUCE_USE_FASTREDUCE = "false";
  static {
    props.put("mapreduce.use.fastreduce", // mapred-default.xml
      MAPREDUCE_USE_FASTREDUCE);
  }
  public static final String MAPREDUCE_USE_MAPRFS = "true";
  static {
    props.put("mapreduce.use.maprfs", // mapred-default.xml
      MAPREDUCE_USE_MAPRFS);
  }
  public static final String MAPRED_USER_JOBCONF_LIMIT = "5242880";
  static {
    props.put("mapred.user.jobconf.limit", // mapred-default.xml
      MAPRED_USER_JOBCONF_LIMIT);
  }
  public static final String MAPRED_USERLOG_LIMIT_KB = "0";
  static {
    props.put("mapred.userlog.limit.kb", // mapred-default.xml
      MAPRED_USERLOG_LIMIT_KB);
  }
  public static final String MAPRED_USERLOG_RETAIN_HOURS = "24";
  static {
    props.put("mapred.userlog.retain.hours", // mapred-default.xml
      MAPRED_USERLOG_RETAIN_HOURS);
  }
  public static final String MAPRED_USERLOG_RETAIN_HOURS_MAX = "168"; // 1 week
  static {
    props.put("mapred.userlog.retain.hours.max", // mapred-default.xml
      MAPRED_USERLOG_RETAIN_HOURS_MAX);
  }
  public static final String MAPRFS_OPENFID2_PREFETCH_BYTES = "0";
  static {
    props.put("maprfs.openfid2.prefetch.bytes", // mapred-default.xml
      MAPRFS_OPENFID2_PREFETCH_BYTES);
  }
  public static final String MAPR_LOCALOUTPUT_DIR = "output";
  static {
    props.put("mapr.localoutput.dir", // mapred-default.xml
      MAPR_LOCALOUTPUT_DIR);
  }
  public static final String MAPR_LOCALSPILL_DIR = "spill";
  static {
    props.put("mapr.localspill.dir", // mapred-default.xml
      MAPR_LOCALSPILL_DIR);
  }
  public static final String MAPR_LOCALVOLUMES_PATH = "/var/mapr/local";
  static {
    props.put("mapr.localvolumes.path", // mapred-default.xml
      MAPR_LOCALVOLUMES_PATH);
  }
  public static final String MAP_SORT_CLASS = "org.apache.hadoop.util.QuickSort";
  static {
    props.put("map.sort.class", // mapred-default.xml
      MAP_SORT_CLASS);
  }
  public static final String TASKTRACKER_HTTP_THREADS = "2";
  static {
    props.put("tasktracker.http.threads", // mapred-default.xml
      TASKTRACKER_HTTP_THREADS);
  }
  public static final String TOPOLOGY_NODE_SWITCH_MAPPING_IMPL = "org.apache.hadoop.net.ScriptBasedMapping";
  static {
    props.put("topology.node.switch.mapping.impl", // core-default.xml
      TOPOLOGY_NODE_SWITCH_MAPPING_IMPL);
  }
  public static final String TOPOLOGY_SCRIPT_NUMBER_ARGS = "100";
  static {
    props.put("topology.script.number.args", // mapred-default.xml
      TOPOLOGY_SCRIPT_NUMBER_ARGS);
  }
  public static final String WEBINTERFACE_PRIVATE_ACTIONS = "false";
  static {
    props.put("webinterface.private.actions", // core-default.xml
      WEBINTERFACE_PRIVATE_ACTIONS);
  }

  public static final String MAPR_MAP_KEYPREFIX_INTS = "1";
  static {
    props.put("mapr.map.keyprefix.ints", MAPR_MAP_KEYPREFIX_INTS);
  }

  public static String getDefault(String name) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetching MapR default for: " + name);
    }
    return props.get(name);
  }

  public static String getPathToMaprHome()
  {
      String maprHome = System.getenv(MAPR_ENV_VAR);

      if (maprHome == null)
      {
          maprHome = System.getProperty(MAPR_PROPERTY_HOME);
          if (maprHome == null)
          {
              maprHome = MAPR_HOME_PATH_DEFAULT;
          }
      }
      return maprHome;
  }

  public static void main(String[] args) {
    // JobConf can not be referenced from a core class
    // Thus immitating load sequence here manually.
    //
    Configuration.addDefaultResource("core-default.xml");
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");

    final Configuration conf = new Configuration();

    if (args.length == 1 && "-dump".equals(args[0])) {
      System.out.println("*** MapR Configuration Dump: BEGIN ***");
      for (Map.Entry<String,String> e : conf) {
        System.out.println(e.getKey() + '=' + e.getValue());
      }
      System.out.println("*** MapR Configuration Dump:   END ***");
    } else if (args.length == 2 && "-key".equals(args[0])) {
      final String key = args[1];
      final String val = conf.get(key);
      if (val != null) {
        System.out.println(val);
      }
    } else {
      System.out.println("Usage: hadoop conf | java MapRConf");
      System.out.println("\t[-dump]       dumps all default key/value pairs");
      System.out.println("\t[-key <key>]  print value for configuration key");
    }
  }
}
