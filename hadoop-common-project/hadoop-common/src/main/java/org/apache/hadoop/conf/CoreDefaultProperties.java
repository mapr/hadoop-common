/**
 * Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.conf;

import org.apache.hadoop.security.authentication.util.MapRSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BaseMapRUtil;
import org.apache.hadoop.util.MapRCommonSecurityUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Background: This class came into existence as part of HDFS unification
 * effort (i.e. creation of a hadoop-common that both MR1 and MR2 can depend on)
 *
 * The "hadoop-common" in this context is the "hadoop-common" project from hadoop2
 * code base in git.(https://github.com/mapr/private-hadoop-common/tree/
 *                           branch-2.3-mapr/hadoop-common-project/hadoop-common)
 *
 * The idea behind this class is as follows:
 *   - The modifications to "hadoop-common" project should be generic, as MapR
 *       wants to submit the changes back to apache community in the future.
 *   - So, MapRConf cannot really live in hadoop-common.
 *     Hence it has been moved into maprfs jar under the name CoreDefaultProperties.
 *   - org.apache.hadoop.conf.Configuration in "hadoop-common" has been modified
 *     to support loading default configuration from a class named
 *     "org.apache.hadoop.conf.CoreDefaultProperties" that extends j.u.Properties.
 *   - Configuration first loads the properties from core-default.xml.
 *   - If CoreDefaultProperties exists in the classpath, then Configuration would
 *     load the configuration from this class before loading the configuration from
 *     core-site.xml.
 *   - Thus MapR version of CoreDefaultProperties would be equivalent to
 *     core-default.xml.
 *
 * Objective for this class:
 *     - Define the MapR specific default FS configuration (core-default.xml)
 *       in one place and make it available for both MR1 and MR2.
 *
 * Author: smarella
 */

public class CoreDefaultProperties extends Properties
{
  private static final Log LOG = LogFactory.getLog(CoreDefaultProperties.class);

  private static final String ShimLoader = "com.mapr.fs.ShimLoader";
  private static final String JVMProperties = "com.mapr.baseutils.JVMProperties";

  static {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(ShimLoader);
      Method loadShims = klass.getDeclaredMethod("load");
      loadShims.invoke(null);
    } catch (ClassNotFoundException err) {
      LOG.info("Cannot find ShimLoader class at classpath");
      err.printStackTrace();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException err) {
      LOG.info("Cannot execute load() method");
      err.printStackTrace();
    }
  }

  private static final boolean isSecurityEnabled;

  static {
    isSecurityEnabled = MapRCommonSecurityUtil.getInstance().isSecurityEnabled();
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(JVMProperties);
      Method initJVMProperties = klass.getDeclaredMethod("init");
      initJVMProperties.invoke(null);
    } catch (ClassNotFoundException err) {
      LOG.info("Cannot find JVMProperties class at classpath");
      err.printStackTrace();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException err) {
      LOG.info("Cannot execute init() method");
      err.printStackTrace();
    }
  }

  private static final Map<String,String> props =
      new HashMap<String,String>();

  public CoreDefaultProperties() {
    this.putAll(props);
  }

  /******* BEGIN :: MAPR defined configuration properties *******/
  public static final String DEFAULT_MAPR_LOCAL_VOL_PATH = "/var/mapr/local";
  static {
    props.put("mapr.home", BaseMapRUtil.getPathToMaprHome());
    props.put("mapr.host", BaseMapRUtil.getMapRHostName());
    props.put("mapr.localvolumes.path", DEFAULT_MAPR_LOCAL_VOL_PATH);
    props.put("mapr.mapred.localvolume.mount.path",
        "${mapr.localvolumes.path}/${mapr.host}/mapred");

    // "mapr.mapred.localvolume.root.dir.name" is set in MapReduceDefaultProperties.
    // In MR1, this is set to "taskTracker". In MR2, this is set to "nodeManager".
    props.put("mapr.mapred.localvolume.root.dir.path",
        "${mapr.mapred.localvolume.mount.path}/${mapr.mapred.localvolume.root.dir.name}");
  }
  /******* END   :: MAPR defined configuration properties *******/

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
    props.put("dfs.namenode.checkpoint.dir", // core-default.xml
        FS_CHECKPOINT_DIR);
  }
  public static final String FS_CHECKPOINT_EDITS_DIR = "${fs.checkpoint.dir}";
  static {
    props.put("dfs.namenode.checkpoint.edits.dir", // core-default.xml
        FS_CHECKPOINT_EDITS_DIR);
  }
  public static final String FS_CHECKPOINT_PERIOD = "3600";
  static {
    props.put("dfs.namenode.checkpoint.period", // core-default.xml
        FS_CHECKPOINT_PERIOD);
  }
  public static final String FS_CHECKPOINT_SIZE = "67108864";
  static {
    props.put("fs.checkpoint.size", // core-default.xml
        FS_CHECKPOINT_SIZE);
  }
  public static final String FS_DEFAULT_NAME = "maprfs:///";
  static {
    props.put("fs.defaultFS", // core-default.xml
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
  public static final String FS_MFS_IMPL = "com.mapr.fs.MFS";
  static {
    props.put("fs.AbstractFileSystem.maprfs.impl", // core-default.xml
        FS_MFS_IMPL);
  }
  public static final String FS_HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
  // Bug 12079: By default, we map the HDFS Impl to MapRFS to provide out-of-the-box
  // compatibility with Hadoop cluster. This can be overridden with '-D' switch.
  static {
    props.put("fs.hdfs.impl", // core-default.xml
        FS_MAPRFS_IMPL);
  }
  static {
    props.put("fs.AbstractFileSystem.hdfs.impl", // core-default.xml
        FS_MFS_IMPL);
  }
  public static final String FS_WEBHDFS_IMPL = "org.apache.hadoop.hdfs.web.WebHdfsFileSystem";
  static {
    props.put("fs.webhdfs.impl", // core-default.xml
        FS_WEBHDFS_IMPL);
  }
  public static final String FS_MAPR_WORKING_DIR = "/user/$USERNAME/";
  static {
    props.put("fs.mapr.working.dir", // core-default.xml
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
    props.put("fs.s3n.blockSize", // core-default.xml
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
    props.put("io.native.lib.available", // core-default.xml
        HADOOP_NATIVE_LIB);
  }
  public static final String HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT = "org.apache.hadoop.net.StandardSocketFactory";
  static {
    props.put("hadoop.rpc.socket.factory.class.default", // core-default.xml
        HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT);
  }

  static {
    if ( isSecurityEnabled ) {
      // set the value for hadoop.security.authentication
      props.put(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, // core-default.xml
          UserGroupInformation.AuthenticationMethod.CUSTOM.name());
      props.put(CommonConfigurationKeys.CUSTOM_RPC_AUTH_METHOD_CLASS_KEY, // core-default.xml
          "org.apache.hadoop.security.rpcauth.MaprAuthMethod");
      props.put(CommonConfigurationKeys.CUSTOM_AUTH_METHOD_PRINCIPAL_CLASS_KEY, // core-default.xml
          "com.mapr.security.MapRPrincipal");

      props.put("hadoop.http.authentication.type",
          "org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler");
      // set hadoop.ssl.enabled to be true
      props.put(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, "true"); // this is a deprecated property in core-default.xml. It is used to enable the SslSocketConnector in HttpServer used by jobtracker, tasktracker etc.

      props.put(CommonConfigurationKeysPublic.LOG_LEVEL_AUTHENTICATOR_CLASS,
          "com.mapr.security.maprauth.MaprAuthenticator");

      props.put(CommonConfigurationKeysPublic.HADOOP_WEBAPPS_CUSTOM_HEADERS_PATH,
          "etc/hadoop/jetty-headers.xml");
    } else {
      props.put(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, // core-default.xml
          UserGroupInformation.AuthenticationMethod.SIMPLE.name());
    }
  }

  static {
    props.put("hadoop.http.authentication.signature.secret", // core-default.xml
        "com.mapr.security.maprauth.MaprSignatureSecretFactory");
  }

  static {
    if ( isSecurityEnabled ) {
      props.put("hadoop.http.authentication.signer.secret.provider", // core-default.xml
          MapRSignerSecretProvider.class.getName());
    } else {
      props.put("hadoop.http.authentication.signer.secret.provider", // core-default.xml
          "random");
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
      props.put("hadoop.rpc.protection", // core-default.xml
          HADOOP_SECURITY_PROTECTION);
    }
  }

  public static final String HADOOP_SECURITY_GROUP_MAPPING = "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback";
  static {
    props.put("hadoop.security.group.mapping", // core-default.xml
        HADOOP_SECURITY_GROUP_MAPPING);
  }

  public static final String MAPR_SECURITY_JAVA_SECURITY_JAR_PATH =
      "/mapr.login.conf";
  static {
    props.put("hadoop.security.java.security.login.config.jar.path", // core-default.xml
        MAPR_SECURITY_JAVA_SECURITY_JAR_PATH);
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
          "SSL_RSA_EXPORT_WITH_RC4_40_MD5," +
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA," +
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA," +
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256," +
          "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256," +
          "TLS_DHE_DSS_WITH_AES_256_CBC_SHA," +
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256," +
          "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256," +
          "TLS_DHE_DSS_WITH_AES_128_CBC_SHA";
  static {
    props.put("hadoop.ssl.exclude.cipher.suites", // core-default.xml
        SSL_EXCLUDE_CIPHER_SUITES);
  }
  public static final String HADOOP_SSL_HOSTNAME_VERIFIER = "DEFAULT";
  static {
    props.put("hadoop.ssl.hostname.verifier", // core-default.xml
        HADOOP_SSL_HOSTNAME_VERIFIER);
  }
  public static final String SSL_EXCLUDE_INSECURE_PROTOCOLS = "SSLv3,TLSv1";
  static {
    props.put("hadoop.ssl.exclude.insecure.protocols", // core-default.xml
        SSL_EXCLUDE_INSECURE_PROTOCOLS);
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
    props.put("dfs.bytes-per-checksum", // core-default.xml
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
  public static final String TOPOLOGY_NODE_SWITCH_MAPPING_IMPL = "org.apache.hadoop.net.ScriptBasedMapping";
  static {
    props.put("net.topology.node.switch.mapping.impl", // core-default.xml
        TOPOLOGY_NODE_SWITCH_MAPPING_IMPL);
  }
  public static final String HTTP_STATICUSER_USER = "unknown";
  static {
    props.put("hadoop.http.staticuser.user", // core-default.xml
        HTTP_STATICUSER_USER);
  }
}

