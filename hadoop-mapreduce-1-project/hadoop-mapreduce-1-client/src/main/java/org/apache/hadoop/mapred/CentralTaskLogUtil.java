/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import java.net.URI;
import java.net.URISyntaxException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is a collection of methods to call out of Apache
 * to keeep changes there at minimum.
 */
public final class CentralTaskLogUtil {

  private static final Configuration DEFAULT_CONF = new Configuration();

  private final static Charset ascii = Charset.forName("US-ASCII");
  private static final byte MAPRCP_CTL_SWITCH_OUTPUT = 0x02;
  private static final byte MAPRCP_CTL_SWITCH_OUTPUT_ARGC = 0x01;
  private static final byte DC3 = 0x13;              /* TODO: configurable ? */
  private static final byte DC1 = 0x11;
  private static final byte COPYRIGHT = (byte)0xA9;

  private static final String PREFIX = "maprfs";

  private static final Log LOG = LogFactory.getLog(CentralTaskLogUtil.class);

  /**
   * Central logging is considered enabled when the root logger
   * has a substring maprfs.
   */
  public static final boolean CENTRAL_LOG =
    System.getProperty("hadoop.root.logger", "INFO,DRFA").contains(PREFIX);

  private static final String USERLOGS_SUBSTR =
                                  File.separator
                                + TaskLog.USERLOGS_DIR_NAME
                                + File.separator;

  private static final String LOCAL_VOLUME_LOGPATH =
      DEFAULT_CONF.get("mapr.localvolumes.path")
    + Path.SEPARATOR + TaskTracker.LOCALHOSTNAME + Path.SEPARATOR
    + DEFAULT_CONF.get("mapr.centrallog.dir", "logs");
  public static final Path USERLOG_ROOT_PATH =
    new Path(LOCAL_VOLUME_LOGPATH + Path.SEPARATOR + "mapred/userlogs");

  private static FileSystem maprFileSystem;

  public static final String getCentralRelativePathStr(String localPathStr) {
    final int userLogDirPos = localPathStr.indexOf(USERLOGS_SUBSTR);
    return   "mapred"
           + USERLOGS_SUBSTR
           + localPathStr.substring(userLogDirPos + USERLOGS_SUBSTR.length());
  }

  public static final String getCentralAbsolutePathStr(String localPathStr) {
    return    LOCAL_VOLUME_LOGPATH  + "/"
            + getCentralRelativePathStr(localPathStr);
  }

  /**
   * Generates a command line portion for pipe from a local path.
   * @param localPathStr    String representing local path.
   * @returns A string to use for piping
   */
  static final String getCentralPipeCmd(
    String localPathStr,
    boolean withLogDirMove)
  {
    final Path localLogDir = new Path(localPathStr).getParent();

    return    TaskTracker.MAPR_INSTALL_DIR
            + "/bin/maprcp - "
            + getCentralUriStr(localPathStr)
            + " -autoflush "
            + (withLogDirMove ?
                 (   " -cpdirents "
                   + localLogDir + " "
                   + getCentralUriStr(localLogDir.toString()))
                 : "" )
            + " 1>/dev/null 2>/dev/null";
  }

  static final String getCentralUriStr(String localPathStr) {
    return PREFIX  + "://" + getCentralAbsolutePathStr(localPathStr);
  }

  static final long getFileLength(File file) {
    long fileLen;
    if (CENTRAL_LOG) {
      try {
        final FileSystem fs = getMaprFileSystem();
        fileLen = fs.getFileStatus(new Path(file.toString())).getLen();
      } catch (IOException e) {
        fileLen = 0L;
      }
    } else {
      fileLen = file.length();
    }
    return fileLen;
  }

  static final boolean mayExist(String localPathStr) {
    try {
      final Path path = new Path(getCentralAbsolutePathStr(localPathStr));
      final boolean exists = getMaprFileSystem().exists(path);

      if (LOG.isDebugEnabled()) {
        LOG.debug("maprfs.exists " + path + ": " + exists);
      }
      return exists;
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unexpected IOException for: " + localPathStr, ioe);
      }
    }

    return true;
  }

  static final InputStream getInputStream(String localPathStr)
     throws IOException
  {
    return CENTRAL_LOG ?
             getMaprFileSystem().open(
               new Path(getCentralAbsolutePathStr(localPathStr))) :
               new FileInputStream(localPathStr);
  }

  static final OutputStream createNewOutputStream(
    String localPathStr,
    short  umask)
    throws IOException
  {
    final FileSystem maprFS = getMaprFileSystem();
    return FileSystem.create(
                        maprFS,
                        new Path(getCentralAbsolutePathStr(localPathStr)),
                        new FsPermission(umask));
  }

  static final boolean existsFile(
    File path)
  {
    if (CENTRAL_LOG) {
      final String localPathStr = getCentralAbsolutePathStr(path.toString());
      return mayExist(localPathStr);
    } else {
      return path.exists();
    }
  }

  static final boolean deleteFile(
    File path,
    boolean recursive)
  {

    try {
      if (CENTRAL_LOG) {
        final String centralPath = getCentralAbsolutePathStr(path.toString());
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Calling mapr.deleteFile: p=" + centralPath + " r=" + recursive);
        }

        return getMaprFileSystem().delete(
          new Path(centralPath), recursive);

      } else {
        final FileSystem localFS = FileSystem.getLocal(DEFAULT_CONF);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Calling loc.deleteFile: p=" + path + " r=" + recursive);
        }
        return localFS.delete(new Path(path.toString()), recursive);
      }
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unexpected IOException for: " + path, ioe);
      }
    }
    return false;
  }

  static final boolean renameFile(
    File srcPath,
    File destPath)
  {
    try {
      if (CENTRAL_LOG) {
        final String centralSrc =
          getCentralAbsolutePathStr(srcPath.toString());
        final String centralDest =
          getCentralAbsolutePathStr(destPath.toString());

        return getMaprFileSystem().rename(
          new Path(centralSrc), new Path(centralDest));
      } else {
        final FileSystem localFS = FileSystem.getLocal(DEFAULT_CONF);
        return localFS.rename(
          new Path(srcPath.toString()),
          new Path(destPath.toString()));
      }
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unexpected IOException for: " + srcPath, ioe);
      }
    }

    return false;
  }

  static void switchTaskLogdir(
    TaskLog.LogdirSwitchable syslogAppender,
    TaskAttemptID taskid,
    boolean isCleanup)
  {
    final File taskLogDir = TaskLog.getAttemptDir(taskid, isCleanup);
    final String centralLogDir =
      CentralTaskLogUtil.getCentralRelativePathStr(taskLogDir.toString());
    syslogAppender.switchLogdir(centralLogDir);
  }

  static void writeSwitchStdStream(
    TaskAttemptID    taskid,
    boolean          isCleanup,
    TaskLog.LogName  filter)
  {

    final PrintStream stream;
    switch (filter) {
    case STDERR:
      stream = System.err;
      break;
    case STDOUT:
      stream = System.out;
      break;
    default:
      System.err.println("writeSwitchStdStream: Infeasible filter: " + filter);
      stream = null;
      return;
    }

    final File taskLogFile = TaskLog.getTaskLogFile(taskid, isCleanup, filter);
    final String taskLogPathStr = getCentralUriStr(taskLogFile.toString());
    final byte[] taskLogPathBytes = taskLogPathStr.getBytes(ascii);

    final ByteBuffer cmdBuf = ByteBuffer.allocate(1 << 13);
    cmdBuf.order(ByteOrder.nativeOrder());       /* ensure native endianness */
    cmdBuf.put(DC3);
    cmdBuf.put("MapR".getBytes(ascii));
    cmdBuf.put(COPYRIGHT);
    cmdBuf.put(MAPRCP_CTL_SWITCH_OUTPUT);
    cmdBuf.put(MAPRCP_CTL_SWITCH_OUTPUT_ARGC);
    cmdBuf.putShort((short)(taskLogPathBytes.length + 1));
    cmdBuf.put(taskLogPathBytes);
    cmdBuf.put((byte)0);                                 /* terminating zero */
    long checksum = 0;
    for (int i = 0; i < cmdBuf.position(); i++) {
      checksum += cmdBuf.get(i);
    }
    cmdBuf.putLong(checksum + DC1);
    cmdBuf.put(DC1);
    stream.write(cmdBuf.array(), 0, cmdBuf.position());
    stream.flush();
  }

  static synchronized FileSystem getMaprFileSystem() throws IOException {
    if (maprFileSystem == null) {
      try {
        maprFileSystem =
          FileSystem.get(new URI(PREFIX + ":///dummy"), DEFAULT_CONF);
      } catch (URISyntaxException use) {
        throw new RuntimeException("Failure to construct maprfs URI", use);
      }
    }
    return maprFileSystem;
  }

  static void prepareLogFiles(
    String logDir,
    String taskUser,
    String taskUserGroup)
    throws IOException
  {
    if (!CENTRAL_LOG) return;

    final FileSystem fs;
    final Path userLogDirPath;

    fs = getMaprFileSystem();
    userLogDirPath = new Path(getCentralAbsolutePathStr(logDir));
    final Path parent = userLogDirPath.getParent();
    final boolean parentExists = fs.exists(parent);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Prepare log files as: "
              + UserGroupInformation.getCurrentUser().getUserName()
              + " for path=" + parent
              + " . Exists=" + parentExists);
    }

    if (!parentExists) {
      fs.mkdirs(parent);
      fs.setOwner(parent, taskUser, taskUserGroup);
    }

    fs.mkdirs(userLogDirPath);
    fs.setOwner(userLogDirPath, taskUser, taskUserGroup);
  }

  static void scheduleCleanup(
    CleanupQueue cleanupQueue,
    String logDir)
    throws IOException
  {
    final FileSystem fs = getMaprFileSystem();
    final Path userLogDirPath =
      new Path(getCentralAbsolutePathStr(logDir)).makeQualified(fs);
    if (LOG.isInfoEnabled()) {
      LOG.info("Deleting user log path " + userLogDirPath);
    }
    final CleanupQueue.PathDeletionContext pdCtx  =
      new CleanupQueue.PathDeletionContext(userLogDirPath, DEFAULT_CONF);
    cleanupQueue.addToQueue(pdCtx);
  }

  static void cleanLocalLog(
    FileSystem lfs,
    JobID jobid,
    TaskLog.LogName log)
  {
    final Path logPattern = new Path(TaskLog.getJobDir(jobid) + "/*/" + log);
    FileStatus[] logStats;
    try {
      logStats = lfs.globStatus(logPattern);
    } catch (IOException ie) {
      logStats = null;
    }
    if (logStats == null) return;
    for (FileStatus logFileStatus : logStats) {
      try {
        lfs.delete(logFileStatus.getPath(), false);
      } catch (IOException ignored) {}
    }
  }

  static FileStatus[] getOldUserLogs() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading old central job logs at: " + USERLOG_ROOT_PATH);
    }
    final FileSystem fs = getMaprFileSystem();
    if (fs.exists(USERLOG_ROOT_PATH)) {
      final FileStatus[] stats = fs.listStatus(USERLOG_ROOT_PATH);
      if (stats == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("userlogs unlistable.");
        }
        return new FileStatus[0];
      }
      return stats;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No central job logs found.");
      }
      return new FileStatus[0];
    }
  }
}
