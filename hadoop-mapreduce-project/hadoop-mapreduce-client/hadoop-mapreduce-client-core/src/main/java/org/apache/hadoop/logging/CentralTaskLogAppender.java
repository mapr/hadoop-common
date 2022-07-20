package org.apache.hadoop.logging;


import java.io.File;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;

/**
 * A simple log4j-appender for the task child's
 * map-reduce system logs.
 *
 */
public class CentralTaskLogAppender extends MaprfsLogAppender {
    private String taskId; //taskId should be managed as String rather than TaskID object
    //so that log4j can configure it from the configuration(log4j.properties).

    private int maxEvents = 0;
    private static final int EVENT_SIZE = 100;

    private Queue<LoggingEvent> tail = null;
    private Boolean isCleanup;

    // System properties passed in from JVM runner
    static final String ISCLEANUP_PROPERTY = "hadoop.tasklog.iscleanup";
    static final String TASKID_PROPERTY = "hadoop.tasklog.taskid";

    @Override
    public void activateOptions() {
        synchronized (this) {
            setOptionsFromSystemProperties();

            if (maxEvents > 0) {
                tail = new LinkedList<LoggingEvent>();
            }
            final String localSyslogName = TaskLog.getTaskLogFile(
                    TaskAttemptID.forName(taskId),
                    isCleanup,
                    TaskLog.LogName.SYSLOG).toString();
            final String centralSyslogName =
                    getCentralRelativePathStr(localSyslogName);
            super.setFile(centralSyslogName);
            super.activateOptions();
        }
    }

    private String getCentralRelativePathStr(String localSyslogName) {
        String userlogs_substr = File.separator+"userlogs"+File.separator;
        final int userLogDirPos = localSyslogName.indexOf(userlogs_substr);
        if( userLogDirPos != -1 ) {
            return getFrameworkType() + userlogs_substr+localSyslogName.substring(userLogDirPos+userlogs_substr.length());
        } else {
            return getFrameworkType() + userlogs_substr+localSyslogName;
        }
    }

    protected String getFrameworkType() {
        return "mapred";
    }

    @Override
    public void setFile(String fileName) {
        LogLog.debug(getClass() + ".setFile is a NOP, fileName=" + fileName);
    }

    /**
     * The Task Runner passes in the options as system properties. Set
     * the options if the setters haven't already been called.
     */
    private synchronized void setOptionsFromSystemProperties() {
        if (isCleanup == null) {
            String propValue = System.getProperty(ISCLEANUP_PROPERTY, "false");
            isCleanup = Boolean.valueOf(propValue);
        }

        if (taskId == null) {
            taskId = System.getProperty(TASKID_PROPERTY);
        }

        setTotalLogFileSize();
    }

    @Override
    public void append(LoggingEvent event) {
        synchronized (this) {
            if (tail == null) {
                super.append(event);
            } else {
                if (tail.size() >= maxEvents) {
                    tail.remove();
                }
                tail.add(event);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (tail != null) {
            for(LoggingEvent event: tail) {
                super.append(event);
            }
        }
        super.close();
    }

    /**
     * Getter/Setter methods for log4j.
     */

    public synchronized String getTaskId() {
        return taskId;
    }

    public synchronized void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public synchronized long getTotalLogFileSize() {
        return maxEvents * EVENT_SIZE;
    }

    public synchronized void setTotalLogFileSize() {
        Long fileSizeLimit = getFileSizeLimit();
        if (fileSizeLimit != null && fileSizeLimit > 0) {
            this.maxEvents = (int) (fileSizeLimit / EVENT_SIZE);
        }
    }

    /**
     * By default, there is no limit. Subclasses can override this
     * behavior.
     */
    protected Long getFileSizeLimit() {
        return null;
    }

    /**
     * Set whether the task is a cleanup attempt or not.
     *
     * @param isCleanup
     *          true if the task is cleanup attempt, false otherwise.
     */
    public synchronized void setIsCleanup(boolean isCleanup) {
        this.isCleanup = isCleanup;
    }

    /**
     * Get whether task is cleanup attempt or not.
     *
     * @return true if the task is cleanup attempt, false otherwise.
     */
    public synchronized boolean getIsCleanup() {
        return isCleanup;
    }

}
