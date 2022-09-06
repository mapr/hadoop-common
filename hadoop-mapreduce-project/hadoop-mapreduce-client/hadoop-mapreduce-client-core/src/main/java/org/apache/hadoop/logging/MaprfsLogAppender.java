package org.apache.hadoop.logging;

import java.io.Flushable;
import java.io.IOException;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;

import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.BaseMapRUtil;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Base log appender to log into MapRFS
 * it can be used on its own or most likely its children will be used:
 * MaprfsDailyRollingLogAppender ( counterpart of DailyRollingFileAppender)
 * MaprfsRollingLogAppender ( counterpart of RollingFileAppender )
 *
 * This appender provides failover functionality to write using other (non MapRFS appender)
 * in case logs can not be written into MapRFS. LoggingEvent that first encountered an issue will not be written
 * into failover appender
 *
 * @author yufeldman
 *
 */
public class MaprfsLogAppender extends AppenderSkeleton
        implements Flushable, Syncable
{
    private static final FsPermission DEFAULT_PERMISSIONS =
            new FsPermission((short)0644);
    private FSDataOutputStream fsout;
    protected FileSystem maprFS;
    protected URI uri;
    protected String fileName;
    protected String nameHierarchy;
    protected Path fileNamePath;

    protected Appender failoverAppender;
    protected String failoverAppenderName;

    public int BUFFER_SIZE = 8*1024;

    private static String hostName;
    protected boolean immediateFlush = true; // by default is true
    protected boolean immediateSync = false;
    protected long    syncIntervalSeconds = -1; // by default disabled

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1, new SyncSchedulerFactory());

    private ScheduledFuture syncFuture;

    private static final class SyncSchedulerFactory implements ThreadFactory {
        private final ThreadFactory dtf = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = dtf.newThread(r);
            t.setName(t.getName() + "-" + SyncSchedulerFactory.class.getName());
            t.setDaemon(true);
            return t;
        }
    }

    private static final class SyncTask implements Runnable {
        private static final int MAX_CONSECUTIVE_ERRORS = 3;
        private static final String FATAL_MSG =
                "Reached maximum errors. Cancellinng.";

        private final Syncable resource;
        private int errorsUntilCancel;

        private SyncTask(Syncable r) {
            resource = r;
        }

        @Override
        public void run() {
            try {
                LogLog.debug(this + " is being executed on " + resource);
                resource.hsync();
                errorsUntilCancel = MAX_CONSECUTIVE_ERRORS;
            } catch (IOException e) {
                errorsUntilCancel--;
                if (errorsUntilCancel == 0) {
                    LogLog.error(FATAL_MSG, e);
                    throw new RuntimeException(FATAL_MSG , e);
                } else {
                    LogLog.error("SyncTask failed.", e);
                }
            }
        }
    }

    static {
        try {
            hostName = BaseMapRUtil.getMapRHostName();
            if (hostName == null) {
                LogLog.error("Error obtaining host name from MapR configuration");
                InetAddress addr = InetAddress.getLocalHost();
                //Get IP Address
                hostName = addr.getCanonicalHostName().toLowerCase();
            }
        } catch (UnknownHostException e) {
            /**
             * <MAPR_ERROR>
             * Message:Current Host Info can not be found. Defaulting to "localhost"
             * Function:MaprfsLogAppender
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Current Host Info can not be found. Defaulting to \"localhost\"", e);
            hostName = "localhost";
        }

    }
    public static final String DIR_PREFIX = "/var/mapr/local/" + hostName + "/logs/";

    public MaprfsLogAppender(URI uri) {
        try {
            Configuration conf = new Configuration();
            maprFS = FileSystem.get(uri, conf);
            fsout = maprFS.create(new Path(DIR_PREFIX, "defaultLog"), false, BUFFER_SIZE);
        } catch (IOException e) {
            /**
             * <MAPR_ERROR>
             * Message:Could not get MaprFs System or create an OutputStream for logging. Failing over to local logging
             * Function:MaprfsLogAppender
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Could not get MaprFs System or create an OutputStream for logging. Failing over to local logging", e);
            failoverToLocalLogs(e);
        }
    }

    public MaprfsLogAppender() {
    }

    public void setFailoverAppender(String failoverAppenderName) {
        this.failoverAppenderName = failoverAppenderName;
    }

    public void setURI(String uriStr) {
        try {
            uri = new URI(uriStr);
        } catch (URISyntaxException e) {
            /**
             * <MAPR_ERROR>
             * Message:URI for maprFs system is not valid: <URI>
             * Function:MaprfsLogAppender.setURI()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("URI for maprFs system is not valid: " + uriStr, e);
        }
    }

    public
    void setImmediateFlush(boolean value) {
        immediateFlush = value;
    }

    /**
     Returns value of the <b>ImmediateFlush</b> option.
     */
    public
    boolean getImmediateFlush() {
        return immediateFlush;
    }

    public void setImmediateSync(boolean value) {
        immediateSync = value;
    }

    public boolean getImmediateSync() {
        return immediateSync;
    }

    public void setSyncIntervalSeconds(long value) {
        syncIntervalSeconds = value;
    }

    public long getSyncIntervalSeconds() {
        return syncIntervalSeconds;
    }

    public void setFile(String fileName) {
        // let's assume that it has file name + some path to identify from which application this log is coming
        if ( fileName != null ) {
            String [] paths = fileName.split("/");
            if ( paths!= null && paths.length > 0 ) {
                this.fileName = paths[paths.length -1];
            }
            int indexOfSlash = fileName.lastIndexOf("/");
            if ( indexOfSlash > 0 ) {
                this.nameHierarchy = fileName.substring(0, fileName.lastIndexOf("/"));
            } else {
                this.nameHierarchy = "defaultapp";
            }
        } else {
            /**
             * <MAPR_ERROR>
             * Message:Log file name is null. Check log4j configuration "File" property.
             * Function:MaprfsLogAppender.setFile()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Log file name is null. Check log4j configuration \"File\" property.");
        }
    }

    public String getFile() {
        final Path finalPath = new Path(DIR_PREFIX, nameHierarchy);
        final Path fileNamePath = new Path(finalPath, fileName);
        return fileNamePath.toString();
    }

    @VisibleForTesting
    protected void setFS(FileSystem fs) {
        this.maprFS = fs;
    }

    @Override
    public void activateOptions() {
        if ( fsout != null ) {
            closeFile();
        }

        if (syncFuture != null) {
            syncFuture.cancel(false);
            syncFuture = null;
        }
        closed = false;
        if (uri != null && fileName != null) {
            try {
                Configuration conf = new Configuration();
                if ( maprFS == null ) {
                    maprFS = FileSystem.get(uri, conf);
                }
                if ( !createDirs() ) {
                    // error should back out to regular logging
                    failoverToLocalLogs(null);
                }
                Path finalPath = new Path(DIR_PREFIX, nameHierarchy);
                fileNamePath = new Path(finalPath, fileName);
                if ( maprFS.exists(fileNamePath) ) {
                    fsout = maprFS.append(fileNamePath, BUFFER_SIZE);
                    writeHeader();
                } else {
                    createFile(fileNamePath);
                }

                LogLog.debug(
                        this + ".activateOptions:"
                                + " fs=" + fsout
                                + " if=" + immediateFlush
                                + " is=" + immediateSync + " syncInterval=" + syncIntervalSeconds
                                + " fileNamePath=" + fileNamePath);

                if (fsout != null
                        && !immediateFlush
                        && !immediateSync
                        && syncIntervalSeconds > 0)
                {
                    syncFuture = scheduler.scheduleAtFixedRate(
                            new SyncTask(fsout),
                            syncIntervalSeconds,
                            syncIntervalSeconds,
                            TimeUnit.SECONDS);
                    LogLog.debug(
                            "syncTask=" + syncFuture
                                    + " shutDown=" + scheduler.isShutdown());
                }
            } catch (IOException e) {
                /**
                 * <MAPR_ERROR>
                 * Message:Could not get MaprFs System or create an OutputStream for logging. Failing over to local logging
                 * Function:MaprfsLogAppender.activateOptions()
                 * Meaning:An error occurred.
                 * Resolution:Contact technical support.
                 * </MAPR_ERROR>
                 */
                LogLog.error("Could not get MaprFs System or create an OutputStream for logging. Failing over to local logging", e);
                failoverToLocalLogs(e);
            }
        } else {
            // no filename or URI provided fail over to local logging
            /**
             * <MAPR_ERROR>
             * Message:No URI or File provided. Failing over to local logging
             * Function:MaprfsLogAppender.activateOptions()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("No URI or File provided. Failing over to local logging");
            if ( fileName == null ) {
                /**
                 * <MAPR_ERROR>
                 * Message:Log file name is null. Check log4j configuration "File" property.
                 * Function:MaprfsLogAppender.activateOptions()
                 * Meaning:An error occurred.
                 * Resolution:Contact technical support.
                 * </MAPR_ERROR>
                 */
                LogLog.error("Log file name is null. Check log4j configuration \"File\" property.");
            }
            failoverToLocalLogs(null);

        }
    }

    @Override
    public void flush() {
        if (fsout == null) return;
        try {
            fsout.flush();
        } catch (IOException e) {
            LogLog.error(
                    "Could not write to: "
                            + fsout.getWrappedStream().toString()
                            + ". Failing over to local logging", e);
        } catch (Throwable t) {
            LogLog.error("Fatal error while trying to write to maprfs.", t);
        }
    }

    @Override
    @Deprecated
    public void sync() throws IOException {
        // Sync has been deprecated in favor of hsync.
        hsync();
    }

    @Override
    public void hsync() {
        if (fsout == null) return;
        try {
            fsout.hsync();
        } catch (IOException e) {
            LogLog.error(
                    "Could not write to: "
                            + fsout.getWrappedStream().toString()
                            + ". Failing over to local logging", e);
        } catch (Throwable t) {
            LogLog.error("Fatal error while trying to write to maprfs.", t);
        }
    }

    @Override
    public void hflush() {
        if (fsout == null) return;
        try {
            fsout.hflush();
        } catch (IOException e) {
            LogLog.error(
                    "Could not write to: "
                            + fsout.getWrappedStream().toString()
                            + ". Failing over to local logging", e);
        } catch (Throwable t) {
            LogLog.error("Fatal error while trying to write to maprfs.", t);
        }
    }

    protected void createFile(Path filePath) throws IOException {
        LogLog.debug(new Date(System.currentTimeMillis()) + ": create file: " + filePath);
        fsout = maprFS.create(filePath, true, BUFFER_SIZE);
        writeHeader();
        maprFS.setPermission(filePath, getLogFilePermission());
    }

    @Override
    protected void append(LoggingEvent event) {
        if (fsout == null) return;
        try {
            fsout.write(this.layout.format(event).getBytes());

            if(layout.ignoresThrowable()) {
                String[] s = event.getThrowableStrRep();
                if (s != null) {
                    int len = s.length;
                    for(int i = 0; i < len; i++) {
                        fsout.write(s[i].getBytes());
                        fsout.write(Layout.LINE_SEP.getBytes());
                    }
                }
            }
            if (immediateSync) {
                fsout.hsync();
            } else if (immediateFlush) {
                fsout.flush();
            }
        } catch (IOException e) {
            /**
             * <MAPR_ERROR>
             * Message:Could not write to: <log>. Failing over to local logging
             * Function:MaprfsLogAppender.append()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Could not write to: " + fsout.getWrappedStream().toString() + ". Failing over to local logging", e);
            failoverToLocalLogs(e);
        } catch (Throwable t) {
            /**
             * <MAPR_ERROR>
             * Message:Fatal error while trying to write to maprfs. Failing over to local logging
             * Function:MaprfsLogAppender.append()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Fatal error while trying to write to maprfs. Failing over to local logging", t);
            failoverToLocalLogs(t);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if(closed) {
                return;
            }
            shutdownScheduler();
            closeFile();
        } catch (Throwable t) {
            /**
             * <MAPR_ERROR>
             * Message:Error while closing: <log>
             * Function:MaprfsLogAppender.close()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Fatal error while trying to write to maprfs.", t);
        } finally {
            closed = true;
        }

    }

    protected void closeFile() {
        if (fsout == null) return;
        try {
            writeFooter();
            fsout.close();
        } catch (IOException e) {
            /**
             * <MAPR_ERROR>
             * Message:Error while closing: <log>
             * Function:MaprfsLogAppender.closeFile()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Error while closing.", e);
        } finally {
            fsout = null;
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    private boolean createDirs() throws IOException {
        if ( !maprFS.exists(new Path(DIR_PREFIX)) ) {
            throw new IOException(DIR_PREFIX + " does not exist");
        }

        if ( nameHierarchy == null || nameHierarchy.isEmpty() ) {
            return true; // nothing to create
        }
        Path finalPath = new Path(DIR_PREFIX, nameHierarchy);
        if ( !maprFS.exists(finalPath) ) {
            maprFS.mkdirs(finalPath);
        }

        return true;
    }

    protected
    void writeFooter() {
        if(layout != null) {
            String f = layout.getFooter();
            if(f != null && fsout != null) {
                try {
                    fsout.write(f.getBytes());
                    fsout.flush();
                } catch (IOException e) {
                    /**
                     * <MAPR_ERROR>
                     * Message:Could not write footer to: <log>
                     * Function:MaprfsLogAppender.writeFooter()
                     * Meaning:An error occurred.
                     * Resolution:Contact technical support.
                     * </MAPR_ERROR>
                     */
                    LogLog.error("Could not write footer to: " + fsout.getWrappedStream().toString(), e);
                    // no reason for failover yet
                }
            }
        }
    }

    /**
     Write a header as produced by the embedded layout's {@link
    Layout#getHeader} method.  */
    protected
    void writeHeader() {
        if(layout != null) {
            String h = layout.getHeader();
            if(h != null && fsout != null) {
                try {
                    fsout.write(h.getBytes());
                } catch (IOException e) {
                    /**
                     * <MAPR_ERROR>
                     * Message:Could not write header to: <log>
                     * Function:MaprfsLogAppender.writeHeader()
                     * Meaning:An error occurred.
                     * Resolution:Contact technical support.
                     * </MAPR_ERROR>
                     */
                    LogLog.error("Could not write header to: " + fsout.getWrappedStream().toString(), e);
                    // no reason for failover yet
                } catch (Throwable t) {
                    /**
                     * <MAPR_ERROR>
                     * Message:Fatal error while trying to write to maprfs.
                     * Function:MaprfsLogAppender.writeHeader()
                     * Meaning:An error occurred.
                     * Resolution:Contact technical support.
                     * </MAPR_ERROR>
                     */
                    LogLog.error("Fatal error while trying to write to maprfs.", t);
                }
            }
        }
    }

    protected long getFileSize() throws IOException {
        return fsout == null ? -1L : fsout.size();
    }

    protected synchronized void failoverToLocalLogs(Throwable reason) {
        try {
            shutdownScheduler();
            if ( failoverAppenderName != null ) {
                // need to failover to this one
                Appender failoverApp = getAllLoggersWithThisAppender(Logger.getRootLogger());
                if ( failoverApp != null ) {
                    // remove for all loggers including root one
                    Enumeration<Logger> loggers = Logger.getRootLogger().getLoggerRepository().getCurrentLoggers();
                    while ( loggers.hasMoreElements() ) {
                        Logger loggerA = loggers.nextElement();
                        if ( loggerA.getAppender(this.getName()) != null ) {
                            loggerA.removeAppender(this);
                            loggerA.addAppender(failoverApp);
                        }
                    }
                    Logger rootLogger = Logger.getRootLogger();
                    if ( rootLogger.getAppender(this.getName()) != null ) {
                        rootLogger.removeAppender(this);
                        rootLogger.addAppender(failoverApp);
                    }

                } else {
                    /**
                     * <MAPR_ERROR>
                     * Message:No valid failover appender specified
                     * Function:MaprfsLogAppender.failoverToLocalLogs()
                     * Meaning:An error occurred.
                     * Resolution:Contact technical support.
                     * </MAPR_ERROR>
                     */
                    LogLog.error("Failover appender not specified", reason);
                }
            }
        } finally {
            LogLog.warn(this + ": closed and disabled due to errors.", reason);
            close();  // close this appender as it has failed.
        }
    }

    private Appender getAllLoggersWithThisAppender(Logger logger) {
        Appender failoverApp = logger.getAppender(failoverAppenderName);
        if ( failoverApp != null ) {
            return failoverApp;
        }
        Enumeration<Logger> loggers = logger.getLoggerRepository().getCurrentLoggers();
        while ( loggers.hasMoreElements() ) {
            Logger loggerA = loggers.nextElement();
            failoverApp = loggerA.getAppender(failoverAppenderName);
            if ( failoverApp != null ) {
                return failoverApp;
            } /*else {
        return (getAllLoggersWithThisAppender(loggerA));
      } */
        }
        return null;
    }

    private void shutdownScheduler() {
        scheduler.shutdown();
        for (int i = 0; !scheduler.isShutdown() && i < 10; i++) {
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                    LogLog.warn(
                            this + ": Could not stop the maprfs syncer after " + i
                                    + " attempts. Retrying ...");
                }
            } catch (InterruptedException e) {
                LogLog.warn(this + ": Spurious wakeup: rechecking isShutdown.");
            }
        }
    }

    protected FsPermission getLogFilePermission() {
        return DEFAULT_PERMISSIONS;
    }
}

