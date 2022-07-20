package org.apache.hadoop.logging;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * RollingLogAppender for YARN tasks.
 *
 * This code is copied from MaprfsRollingLogAppender. In order to make it work
 * for Hadoop, we need to reuse the logic in
 * <code>YarnCentralTaskLogAppender#activateOptions</code>. Since we cannot
 * inherit from 2 classes, and the code is fairly small, we did not refactor.
 *
 */
public class YarnCentralTaskRollingLogAppender extends YarnCentralTaskLogAppender {
    /**
     * The default maximum file size is 10MB.
     */
    protected long maxFileSize = 10*1024*1024;

    /**
     * There is one backup file by default.
     */
    protected int  maxBackupIndex  = 1;

    private long nextRollover = 0;

    /**
     * This method is overridden to disable the max event based log file
     * truncation in CentralTaskLogAppender. The file size limit is taken
     * care by this rolling appender.
     */
    @Override
    protected synchronized Long getFileSizeLimit() {
        return null;
    }

    /**
     * Returns the value of the <b>MaxBackupIndex</b> option.
     */
    public int getMaxBackupIndex() {
        return maxBackupIndex;
    }

    /**
     * Get the maximum size that the output file is allowed to reach
     * before being rolled over to backup files.
     */
    public
    long getMaximumFileSize() {
        return maxFileSize;
    }

    /**
     Set the maximum number of backup files to keep around.

     <p>The <b>MaxBackupIndex</b> option determines how many backup
     files are kept before the oldest is erased. This option takes
     a positive integer value. If set to zero, then there will be no
     backup files and the log file will be truncated when it reaches
     <code>MaxFileSize</code>.
     */
    public  void setMaxBackupIndex(int maxBackups) {
        this.maxBackupIndex = maxBackups;
    }

    /**
     Set the maximum size that the output file is allowed to reach
     before being rolled over to backup files.

     <p>This method is equivalent to {@link #setMaxFileSize} except
     that it is required for differentiating the setter taking a
     <code>long</code> argument from the setter taking a
     <code>String</code> argument by the JavaBeans {@link
    java.beans.Introspector Introspector}.

     @see #setMaxFileSize(String)
     */
    public void setMaximumFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }


    /**
     Set the maximum size that the output file is allowed to reach
     before being rolled over to backup files.

     <p>In configuration files, the <b>MaxFileSize</b> option takes an
     long integer in the range 0 - 2^63. You can specify the value
     with the suffixes "KB", "MB" or "GB" so that the integer is
     interpreted being expressed respectively in kilobytes, megabytes
     or gigabytes. For example, the value "10KB" will be interpreted
     as 10240.
     */
    public void setMaxFileSize(String value) {
        maxFileSize = OptionConverter.toFileSize(value, maxFileSize + 1);
    }


    @Override
    public void append(LoggingEvent event) {
        super.append(event);
        try {
            final long size = getFileSize();
            if (size > maxFileSize && size > nextRollover) {
                nextRollover = size + maxFileSize;
                rollOver();
            }
        } catch (IOException e) {
            /**
             * <MAPR_ERROR>
             * Message:Could not write to: <log>. Failing over to local logging
             * Function:MaprfsRollingLogAppender.append()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Could not write to: " + fileNamePath + ". Failing over to local logging", e);
            failoverToLocalLogs(e);
        } catch (Throwable t) {
            /**
             * <MAPR_ERROR>
             * Message:Fatal error while trying to write to maprfs. Failing over to local logging
             * Function:MaprfsRollingLogAppender.append()
             * Meaning:An error occurred.
             * Resolution:Contact technical support.
             * </MAPR_ERROR>
             */
            LogLog.error("Fatal error while trying to write to maprfs. Failing over to local logging", t);
            failoverToLocalLogs(t);
        }
    }

    /**
     * Helper method to close a file to where logs are written and open new file
     * @throws IOException
     */
    private void rollOver() throws IOException {
        if ( maxBackupIndex > 0 ) {
            // need to remove old copy of a file
            if ( maprFS.exists(new Path(fileNamePath.toString() + '.' + maxBackupIndex))) {
                maprFS.delete(new Path(fileNamePath.toString() + '.' + maxBackupIndex), true);
            }
            // need to rename left over copies (n-1 -> n, n-2 -> n-1, ..., 1 -> 2)
            for ( int i = maxBackupIndex -1 ; i > 0; i--) {
                if ( maprFS.exists(new Path(fileNamePath.toString() + '.' + i))) {
                    // TODO log warn if rename failed
                    maprFS.rename(new Path(fileNamePath.toString() + '.' + i), new Path(fileNamePath.toString() + '.' + (i+1)));
                }
            }
            // need to close current file
            closeFile(); // to write footer along with file closure

            if ( maprFS.rename(fileNamePath, new Path(fileNamePath.toString() + '.' + 1)) ) {
                // create new file
                createFile(fileNamePath);
                nextRollover = 0;
            } else {
                // continue appending to existing file
                /**
                 * <MAPR_ERROR>
                 * Message:Unable to rename from: <filename> to: <filename>.
                 * Function:MaprfsRollingLogAppender.append()
                 * Meaning:An error occurred.
                 * Resolution:Contact technical support.
                 * </MAPR_ERROR>
                 */
                LogLog.error("Unable to rename from: " + fileNamePath + " to: " + fileNamePath.toString() + '.' + 1);
                maprFS.append(fileNamePath);
            }
        }
    }
}

