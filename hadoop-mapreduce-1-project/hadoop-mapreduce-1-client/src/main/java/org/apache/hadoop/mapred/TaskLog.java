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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.FileReader;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 * This class is for Map/Reduce internal use only.
 * 
 */
public class TaskLog {

  public interface LogdirSwitchable {
    public void switchLogdir(String dirName);
    public LogName getLogname();
    public void close();
  }

  private static final Log LOG =
    LogFactory.getLog(TaskLog.class);

  static final String USERLOGS_DIR_NAME = "userlogs";

  private static final File LOG_DIR = 
    new File(getBaseLogDir(), USERLOGS_DIR_NAME).getAbsoluteFile();
  
  // localFS is set in (and used by) writeToIndexFile()
  static LocalFileSystem localFS = null;
  static {
    if (!CentralTaskLogUtil.CENTRAL_LOG && !LOG_DIR.exists()) {
      LOG_DIR.mkdirs();
    }
  }

  /* Hadoop-0.20.2 api */
  public static File getTaskLogFile(TaskAttemptID taskid, LogName filter) {
    return getTaskLogFile(taskid, false, filter);
  }

  public static File getTaskLogFile(TaskAttemptID taskid, boolean isCleanup,
      LogName filter) {
    return new File(getAttemptDir(taskid, isCleanup), filter.toString());
  }

  /**
   * Get the real task-log file-path
   * 
   * @param location Location of the log-file. This should point to an
   *          attempt-directory.
   * @param filter
   * @return
   * @throws IOException
   */
  static String getRealTaskLogFilePath(String location, LogName filter)
      throws IOException {
    return FileUtil.makeShellPath(new File(location, filter.toString()));
  }

  static class LogFileDetail {
    final static String LOCATION = "LOG_DIR:";
    String location;
    long start;
    long length;
  }

  static Map<LogName, LogFileDetail> getDefaultLogDetails(
      TaskAttemptID taskid, boolean isCleanup)
  {
    Map<LogName, LogFileDetail> allLogsFileDetails =
        new HashMap<LogName, LogFileDetail>();
    for (LogName log : LogName.values()) {
      final LogFileDetail lfd = new LogFileDetail();
      lfd.location = getAttemptDir(taskid, isCleanup).toString();
      lfd.start = 0L;
      lfd.length = new File(lfd.location, log.toString()).length();
      allLogsFileDetails.put(log, lfd);
    }
    return allLogsFileDetails;
  }

  static Map<LogName, LogFileDetail> getAllLogsFileDetails(
      TaskAttemptID taskid, boolean isCleanup, boolean central)
  throws IOException {

    Map<LogName, LogFileDetail> allLogsFileDetails =
        new HashMap<LogName, LogFileDetail>();

    /* Central Logging never updates log.index, no need to parse it */
    if (central) {
      for (LogName filter : LogName.values()) {
        final LogFileDetail centralLogFileDetail = new LogFileDetail();
        centralLogFileDetail.location =
          CentralTaskLogUtil.getCentralAbsolutePathStr(
            getAttemptDir(taskid, isCleanup).toString());
        centralLogFileDetail.start = 0;
        centralLogFileDetail.length = CentralTaskLogUtil.getFileLength(
            new File(centralLogFileDetail.location, filter.toString()));
        allLogsFileDetails.put(filter, centralLogFileDetail);
      }
      return allLogsFileDetails;
    }

    File indexFile = getIndexFile(taskid, isCleanup);
    BufferedReader fis = new BufferedReader(new FileReader(indexFile));
    //the format of the index file is
    //LOG_DIR: <the dir where the task logs are really stored>
    //stdout:<start-offset in the stdout file> <length>
    //stderr:<start-offset in the stderr file> <length>
    //syslog:<start-offset in the syslog file> <length>
    String str = fis.readLine();
    if (str == null) { //the file doesn't have anything
      fis.close();
      throw new IOException ("Index file for the log of " + taskid+" doesn't exist.");
    }
    String loc = str.substring(str.indexOf(LogFileDetail.LOCATION)+
        LogFileDetail.LOCATION.length());
    //special cases are the debugout and profile.out files. They are guaranteed
    //to be associated with each task attempt since jvm reuse is disabled
    //when profiling/debugging is enabled
    for (LogName filter : new LogName[] { LogName.DEBUGOUT, LogName.PROFILE }) {
      LogFileDetail l = new LogFileDetail();
      l.location = loc;
      l.length = new File(l.location, filter.toString()).length();
      l.start = 0;
      allLogsFileDetails.put(filter, l);
    }
    str = fis.readLine();
    while (str != null) {
      LogFileDetail l = new LogFileDetail();
      l.location = loc;
      int idx = str.indexOf(':');
      LogName filter = LogName.valueOf(str.substring(0, idx).toUpperCase());
      str = str.substring(idx + 1);
      String[] startAndLen = str.split(" ");
      l.start = Long.parseLong(startAndLen[0]);

      l.length = Long.parseLong(startAndLen[1]);
      if (l.length == -1L) {
        l.length = new File(l.location, filter.toString()).length();
      }

      allLogsFileDetails.put(filter, l);
      str = fis.readLine();
    }
    fis.close();
    return allLogsFileDetails;
  }
  
  private static File getTmpIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.tmp");
  }

  static File getIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.index");
  }

  /**
   * Obtain the owner of the log dir. This is 
   * determined by checking the job's log directory.
   */
  static String obtainLogDirOwner(TaskAttemptID taskid) throws IOException {
    Configuration conf = new Configuration();
    FileSystem raw = FileSystem.getLocal(conf).getRaw();
    Path jobLogDir = new Path(getJobDir(taskid.getJobID()).getAbsolutePath());
    FileStatus jobStat = raw.getFileStatus(jobLogDir);
    return jobStat.getOwner();
  }

  static String getBaseLogDir() {
    return System.getProperty("hadoop.log.dir");
  }

  static File getAttemptDir(TaskAttemptID taskid, boolean isCleanup) {
    String cleanupSuffix = isCleanup ? ".cleanup" : "";
    return getAttemptDir(taskid.getJobID().toString(), 
        taskid.toString() + cleanupSuffix);
  }
  
  static File getAttemptDir(String jobid, String taskid) {
    // taskid should be fully formed and it should have the optional 
    // .cleanup suffix
    // TODO(todd) should this have cleanup suffix?
    return new File(getJobDir(jobid), taskid);
  }

  static final List<LogName> LOGS_TRACKED_BY_INDEX_FILES =
      Arrays.asList(LogName.STDOUT, LogName.STDERR, LogName.SYSLOG);

  private static TaskAttemptID currentTaskid;

  /**
   * Map to store previous and current lengths.
   */
  private static Map<LogName, Long[]> logLengths =
      new HashMap<LogName, Long[]>();
  static {
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      logLengths.put(logName, new Long[] { Long.valueOf(0L),
          Long.valueOf(0L) });
    }
  }
  
  static void createTaskAttemptDir(TaskAttemptID currentTaskid, 
      boolean isCleanup) throws IOException {
    final FileSystem fs;

    if (localFS == null) { // set localFS once
      localFS = FileSystem.getLocal(new Configuration());
    }

    // Check if task log dir is present. Create it with permission 2750
    File attemptLogDir = getAttemptDir(currentTaskid, isCleanup);

    final Path logDirPath =
      CentralTaskLogUtil.CENTRAL_LOG ?
        new Path(CentralTaskLogUtil.getCentralAbsolutePathStr(
              attemptLogDir.toString())) :
        new Path(attemptLogDir.getCanonicalPath());

    fs = CentralTaskLogUtil.CENTRAL_LOG ?
           CentralTaskLogUtil.getMaprFileSystem() :
           localFS;
    if (!fs.exists(logDirPath)) {
      if (LOG.isInfoEnabled())
        LOG.info("Creating task log directory " + attemptLogDir);
      boolean res = fs.mkdirs(logDirPath);
      if (res) {
        fs.setPermission(logDirPath, new FsPermission((short)0750));
      } else {
        throw new IOException("Failed to create " + attemptLogDir);
      }
    }
  }

  static synchronized
  void writeToIndexFile(String logLocation,
                        TaskAttemptID currentTaskid, boolean isCleanup,
                        Map<LogName, Long[]> lengths) throws IOException {
    // To ensure atomicity of updates to index file, write to temporary index
    // file first and then rename.
    File tmpIndexFile = getTmpIndexFile(currentTaskid, isCleanup);

    final short umask = 0644;
    final OutputStream os;
    if (CentralTaskLogUtil.CENTRAL_LOG) {
      os = CentralTaskLogUtil.createNewOutputStream(
             tmpIndexFile.toString(), umask);
      final String localDirStr =
        getAttemptDir(currentTaskid, isCleanup).toString();
      logLocation = CentralTaskLogUtil.getCentralAbsolutePathStr(localDirStr);
    } else {
      os = new BufferedOutputStream(
                 SecureIOUtils.createForWrite(tmpIndexFile, umask));
    }

    DataOutputStream dos = new DataOutputStream(os);

    //the format of the index file is
    //LOG_DIR: <the dir where the task logs are really stored>
    //STDOUT: <start-offset in the stdout file> <length>
    //STDERR: <start-offset in the stderr file> <length>
    //SYSLOG: <start-offset in the syslog file> <length>    
    dos.writeBytes(LogFileDetail.LOCATION
        + logLocation
        + "\n");
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      Long[] lens = lengths.get(logName);
      dos.writeBytes(logName.toString() + ":"
          + lens[0].toString() + " "
          + Long.toString(lens[1].longValue() - lens[0].longValue())
          + "\n");}
    dos.close();

    File indexFile = getIndexFile(currentTaskid, isCleanup);
    CentralTaskLogUtil.renameFile(tmpIndexFile, indexFile);
  }

  @SuppressWarnings("unchecked")
  public synchronized static void syncLogs(String logLocation, 
                                           TaskAttemptID taskid,
                                           boolean isCleanup,
                                           boolean segmented) 
  throws IOException {
    System.out.flush();
    System.err.flush();
    final Enumeration<Logger> allLoggers = LogManager.getCurrentLoggers();
    LogdirSwitchable centralAppender = null;
    while (allLoggers.hasMoreElements()) {
      Logger l = allLoggers.nextElement();
      Enumeration<Appender> allAppenders = l.getAllAppenders();
      while (allAppenders.hasMoreElements()) {
        Appender a = allAppenders.nextElement();
        if (a instanceof Syncable) {
          ((Syncable) a).sync();
        } else if (a instanceof Flushable) {
          ((Flushable)a).flush();
        }
        if (centralAppender == null && a instanceof LogdirSwitchable) {
          final LogdirSwitchable lds = (LogdirSwitchable)a;
          if (lds.getLogname() == LogName.SYSLOG) {
            centralAppender = lds;
          }
        }
      }
    }
    if (currentTaskid == null) {
      currentTaskid = taskid;
    }
    // set start and end
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      if (CentralTaskLogUtil.CENTRAL_LOG) {
        logLengths.get(logName)[0] =  0L;
        logLengths.get(logName)[1] = -1L;
      } else {
        if (currentTaskid != taskid) {
          // Set start = current-end
          logLengths.get(logName)[0] = Long.valueOf(new File(
              logLocation, logName.toString()).length());
        }
        // Set current end
        logLengths.get(logName)[1]
          = (segmented
             ? (Long.valueOf
                (new File(logLocation, logName.toString()).length()))
             : -1);
      }
    }

    if (currentTaskid != taskid) {
      if (currentTaskid != null) {
        createTaskAttemptDir(taskid, isCleanup);
        if (CentralTaskLogUtil.CENTRAL_LOG) {
          // 1. close the existing syslog (old task's log4j stream)
          if (centralAppender != null) {
            centralAppender.close(); // flushes Fileclient's GTrace buffers to stdout/stderr
          }

          // 2. clearly mark the end of the current task (so that customers/support/dev don't break their heads)
          String endOfTaskMarker = "-- End of task: " + currentTaskid + " --";
          System.out.println(endOfTaskMarker);
          System.err.println(endOfTaskMarker);

          // 3. flush everything in the stdout/stderr buffers (esp the bytes due to the above GTrace flush)
          System.out.flush();
          System.err.flush();

          // 4. switch the stdout/stderr streams to the new files for the new taskid
          CentralTaskLogUtil.writeSwitchStdStream(
                               taskid, isCleanup, LogName.STDOUT);
          CentralTaskLogUtil.writeSwitchStdStream(
                               taskid, isCleanup, LogName.STDERR);

          // 5. switch to the new syslog (new task's log4j stream). 
          // This generates FC stdout/stderr bytes that should now 
          // safely go into the new taskid's stdout/stderr files 
          if (centralAppender != null) {
            CentralTaskLogUtil.switchTaskLogdir(
                                 centralAppender, taskid, isCleanup);
          }
        } else {
          LOG.info("Starting logging for a new task " + taskid
              + " in the same JVM as that of the first task " + logLocation);
        }
      }
      currentTaskid = taskid;
    }
    writeToIndexFile(logLocation, taskid, isCleanup, logLengths);
  }
  
  /**
   * The filter for userlogs.
   */
  public static enum LogName {
    /** Log on the stdout of the task. */
    STDOUT ("stdout", false),

    /** Log on the stderr of the task. */
    STDERR ("stderr", false),
    
    /** Log on the map-reduce system logs of the task. */
    SYSLOG ("syslog", true),
    
    /** The java profiler information. */
    PROFILE ("profile.out", true),
    
    /** Log the debug script's stdout  */
    DEBUGOUT ("debugout", true);
        
    private final String prefix;
    final boolean optional;

    private LogName(String prefix, boolean optional) {
      this.prefix = prefix;
      this.optional = optional;
    }
    
    @Override
    public String toString() {
      return prefix;
    }
  }

  static class Reader extends InputStream {
    private long bytesRemaining;
    private InputStream file;

    /**
     * Read a log file from start to end positions. The offsets may be negative,
     * in which case they are relative to the end of the file. For example,
     * Reader(taskid, kind, 0, -1) is the entire file and 
     * Reader(taskid, kind, -4197, -1) is the last 4196 bytes. 
     * @param taskid the id of the task to read the log file for
     * @param kind the kind of log to read
     * @param start the offset to read from (negative is relative to tail)
     * @param end the offset to read upto (negative is relative to tail)
     * @param isCleanup whether the attempt is cleanup attempt or not
     * @throws IOException
     */
    public Reader(
      TaskAttemptID taskid, LogName kind, long start, long end,
      Map<LogName,LogFileDetail> allFilesDetails, boolean central)
    throws IOException
    {
      LogFileDetail fileDetail = allFilesDetails.get(kind);
      // calculate the start and stop
      long size = fileDetail.length;
      if (start < 0) {
        start += size + 1;
      }
      if (end < 0) {
        end += size + 1;
      }
      start = Math.max(0, Math.min(start, size));
      end = Math.max(0, Math.min(end, size));
      start += fileDetail.start;
      end += fileDetail.start;
      bytesRemaining = end - start;
      String owner = obtainLogDirOwner(taskid);
      final File fsPath = new File(fileDetail.location, kind.toString());
      file = central ?
               CentralTaskLogUtil.getInputStream(fsPath.toString()):
               SecureIOUtils.openForRead(fsPath, owner, null);

      // skip upto start
      long pos = 0;
      while (pos < start) {
        long result = file.skip(start - pos);
        if (result < 0) {
          bytesRemaining = 0;
          break;
        }
        pos += result;
      }
    }
    
    @Override
    public int read() throws IOException {
      int result = -1;
      if (bytesRemaining > 0) {
        bytesRemaining -= 1;
        result = file.read();
      }
      return result;
    }
    
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      length = (int) Math.min(length, bytesRemaining);
      int bytes = file.read(buffer, offset, length);
      if (bytes > 0) {
        bytesRemaining -= bytes;
      }
      return bytes;
    }
    
    @Override
    public int available() throws IOException {
      return (int) Math.min(bytesRemaining, file.available());
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }

  private static final String bashCommand = "bash";
  private static final String tailCommand = "tail";
  
  /**
   * Get the desired maximum length of task's logs.
   * @param conf the job to look in
   * @return the number of bytes to cap the log files at
   */
  public static long getTaskLogLength(JobConf conf) {
    return conf.getLong("mapred.userlog.limit.kb", -1) * 1024;
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * If the tailLength is 0, the entire output will be saved.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength
                                               ) throws IOException {
    return captureOutAndError(null, null, cmd, stdoutFilename,
                              stderrFilename, tailLength, false);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> setup,
                                                List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength
                                               ) throws IOException {
    return captureOutAndError(setup, null, cmd, stdoutFilename, stderrFilename,
        tailLength, false);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @deprecated pidFiles are no more used. Instead pid is exported to
   *             env variable JVM_PID.
   * @return the modified command that should be run
   */
  @Deprecated
  public static List<String> captureOutAndError(List<String> setup,
                                                List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength,
                                                String pidFileName
                                               ) throws IOException {
    return captureOutAndError(setup, cmd, stdoutFilename, stderrFilename,
        tailLength, false, pidFileName);
  }
  
  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @param useSetsid Should setsid be used in the command or not.
   * @deprecated pidFiles are no more used. Instead pid is exported to
   *             env variable JVM_PID.
   * @return the modified command that should be run
   * 
   */
  @Deprecated
  public static List<String> captureOutAndError(List<String> setup,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetsid,
      String pidFileName
     ) throws IOException {
    return captureOutAndError(setup, null, cmd, stdoutFilename, stderrFilename, tailLength,
        useSetsid);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @param useSetsid Should setsid be used in the command or not.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> setup,
      Map<String,String> setupCmds,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetsid
     ) throws IOException {
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    String mergedCmd = buildCommandLine(setup, setupCmds, cmd,
        stdoutFilename,
        stderrFilename, tailLength,
        useSetsid);
    result.add(mergedCmd.toString());
    return result; 
  }
  
  
  static String buildCommandLine(List<String> setup,
      Map<String,String> setupCmds,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetSid) throws IOException {
    
    String stdout = FileUtil.makeShellPath(stdoutFilename);
    String stderr = FileUtil.makeShellPath(stderrFilename);
    StringBuilder mergedCmd = new StringBuilder();

    // Export the pid of taskJvm to env variable JVM_PID.
    // Currently pid is not used on Windows
    if (!Shell.WINDOWS) {
      mergedCmd.append("export JVM_PID=`echo $$`\n");
    }
    
    if (setup != null) {
      for (String s : setup) {
        mergedCmd.append(s);
        mergedCmd.append("\n");
      }
    }

    // MapR specific
    if (setupCmds != null) {
      for (String c: setupCmds.keySet()) {
        mergedCmd.append (setupCmds.get(c));
        mergedCmd.append(";");
      }
      LOG.info ("MapR: Setup Cmds: " + mergedCmd.toString());
    }

    if (CentralTaskLogUtil.CENTRAL_LOG || tailLength > 0) {
      mergedCmd.append("(");
    } else if (ProcessTree.isSetsidAvailable && useSetSid 
        && !Shell.WINDOWS) {
      mergedCmd.append("exec setsid ");
    } else {
      mergedCmd.append("exec ");
    }
    mergedCmd.append(addCommand(cmd, true));
    mergedCmd.append(" < /dev/null ");
    if (tailLength > 0) {
      mergedCmd.append(" | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      if (CentralTaskLogUtil.CENTRAL_LOG) {
        mergedCmd.append(" | ");
        mergedCmd.append(CentralTaskLogUtil.getCentralPipeCmd(stdout, false));
      } else {
        mergedCmd.append(" >> ");
        mergedCmd.append(stdout);
      }
      mergedCmd.append(" ; exit $PIPESTATUS ) 2>&1 | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      if (CentralTaskLogUtil.CENTRAL_LOG) {
        mergedCmd.append(" | ");
        mergedCmd.append(CentralTaskLogUtil.getCentralPipeCmd(stderr, true));
      } else {
        mergedCmd.append(" >> ");
        mergedCmd.append(stderr);
      }
      mergedCmd.append(" ; exit $PIPESTATUS");
    } else {
      if (CentralTaskLogUtil.CENTRAL_LOG) {
        mergedCmd.append(" | ");
        mergedCmd.append(CentralTaskLogUtil.getCentralPipeCmd(stdout, false));
        mergedCmd.append(" ; exit $PIPESTATUS ) 2>&1 | ");
        mergedCmd.append(CentralTaskLogUtil.getCentralPipeCmd(stderr, true));
        mergedCmd.append(" ; exit $PIPESTATUS");
        if (LOG.isDebugEnabled()) {
          LOG.debug("central logging full pipe: " + mergedCmd.toString());
        }
      } else {
        mergedCmd.append(" 1>> ");
        mergedCmd.append(stdout);
        mergedCmd.append(" 2>> ");
        mergedCmd.append(stderr);
      }
    }

    return mergedCmd.toString();
  }

  /**
   * Add quotes to each of the command strings and
   * return as a single string 
   * @param cmd The command to be quoted
   * @param isExecutable makes shell path if the first 
   * argument is executable
   * @return returns The quoted string. 
   * @throws IOException
   */
  public static String addCommand(List<String> cmd, boolean isExecutable) 
  throws IOException {
    StringBuffer command = new StringBuffer();
    for(String s: cmd) {
    	command.append('\'');
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
    	  command.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
    	  command.append(s);
      }
      command.append('\'');
      command.append(" ");
    }
    return command.toString();
  }
  
  /**
   * Wrap a command in a shell to capture debug script's 
   * stdout and stderr to debugout.
   * @param cmd The command and the arguments that should be run
   * @param debugoutFilename The filename that stdout and stderr
   *  should be saved to.
   * @return the modified command that should be run
   * @throws IOException
   */
  public static List<String> captureDebugOut(List<String> cmd, 
                                             File debugoutFilename
                                            ) throws IOException {
    String debugout = FileUtil.makeShellPath(debugoutFilename);
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    StringBuffer mergedCmd = new StringBuffer();
    mergedCmd.append("exec ");
    boolean isExecutable = true;
    for(String s: cmd) {
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
        mergedCmd.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
        mergedCmd.append(s);
      }
      mergedCmd.append(" ");
    }
    mergedCmd.append(" < /dev/null ");
    mergedCmd.append(" >");
    mergedCmd.append(debugout);
    mergedCmd.append(" 2>&1 ");
    result.add(mergedCmd.toString());
    return result;
  }
  
  public static File getUserLogDir() {  
    return LOG_DIR;
  }
  
  /**
   * Get the user log directory for the job jobid.
   * 
   * @param jobid string representation of the jobid
   * @return user log directory for the job
   */
  public static File getJobDir(String jobid) {
    return new File(getUserLogDir(), jobid);
  }
  
  /**
   * Get the user log directory for the job jobid.
   * 
   * @param jobid the jobid object
   * @return user log directory for the job
   */
  public static File getJobDir(JobID jobid) {
    return getJobDir(jobid.toString());
  }
} // TaskLog
