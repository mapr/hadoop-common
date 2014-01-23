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

import java.io.IOException;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

class JSPUtil {
  static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";
  static final String COUNTER_LIMIT_STR = "Exceeded counters limit";

  //LRU based cache
  private static final Map<String, JobInfo> jobHistoryCache = 
    new LinkedHashMap<String, JobInfo>(); 

  private static final Log LOG = LogFactory.getLog(JSPUtil.class);

  /**
   * Wraps the {@link JobInProgress} object and contains boolean for
   * 'job view access' allowed or not.
   * This class is only for usage by JSPs and Servlets.
   */
  static class JobWithViewAccessCheck {
    private JobInProgress job = null;
    
    // true if user is authorized to view this job
    private boolean isViewAllowed = true;

    JobWithViewAccessCheck(JobInProgress job) {
      this.job = job;
    }

    JobInProgress getJob() {
      return job;
    }

    boolean isViewJobAllowed() {
      return isViewAllowed;
    }

    void setViewAccess(boolean isViewAllowed) {
      this.isViewAllowed = isViewAllowed;
    }
  }

  /**
   * Validates if current user can view the job.
   * If user is not authorized to view the job, this method will modify the
   * response and forwards to an error page and returns Job with
   * viewJobAccess flag set to false.
   * @return JobWithViewAccessCheck object(contains JobInProgress object and
   *         viewJobAccess flag). Callers of this method will check the flag
   *         and decide if view should be allowed or not. Job will be null if
   *         the job with given jobid doesnot exist at the JobTracker.
   */
  public static JobWithViewAccessCheck checkAccessAndGetJob(final JobTracker jt,
      JobID jobid, HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    final JobInProgress job = jt.getJob(jobid);
    JobWithViewAccessCheck myJob = new JobWithViewAccessCheck(job);

    String user = request.getRemoteUser();
    if (user != null && job != null && jt.areACLsEnabled()) {
      final UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(user);
      try {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws IOException, ServletException {

            // checks job view permission
            jt.getACLsManager().checkAccess(job, ugi,
                Operation.VIEW_JOB_DETAILS);
            return null;
          }
        });
      } catch (AccessControlException e) {
        String errMsg = "User " + ugi.getShortUserName() +
            " failed to view " + jobid + "!<br /><br />" + e.getMessage() +
            "<hr><a href=\"jobtracker.jsp\">Go back to JobTracker</a><br />";
        JSPUtil.setErrorAndForward(errMsg, request, response);
        myJob.setViewAccess(false);
      } catch (InterruptedException e) {
        String errMsg = " Interrupted while trying to access " + jobid +
        "<hr><a href=\"jobtracker.jsp\">Go back to JobTracker</a><br />";
        JSPUtil.setErrorAndForward(errMsg, request, response);
        myJob.setViewAccess(false);
      }
    }
    return myJob;
  }

  /**
   * Sets error code SC_UNAUTHORIZED in response and forwards to
   * error page which contains error message and a back link.
   */
  public static void setErrorAndForward(String errMsg,
      HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("error.msg", errMsg);
    RequestDispatcher dispatcher = request.getRequestDispatcher(
        "/job_authorization_error.jsp");
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    dispatcher.forward(request, response);
  }

  /**
   * Method used to process the request from the job page based on the 
   * request which it has received. For example like changing priority.
   * 
   * @param request HTTP request Object.
   * @param response HTTP response object.
   * @param tracker {@link JobTracker} instance
   * @throws IOException
   * @throws InterruptedException 
   * @throws ServletException 
   */
  public static void processButtons(HttpServletRequest request,
      HttpServletResponse response, final JobTracker tracker)
      throws IOException, InterruptedException, ServletException {

	String user = request.getRemoteUser();
    if (privateActionsAllowed(tracker.conf)
        && request.getParameter("killJobs") != null) {
        String[] jobs = request.getParameterValues("jobCheckBox");
        if (jobs != null) {
          boolean notAuthorized = false;
          String errMsg = "User " + user
              + " failed to kill the following job(s)!<br /><br />";

          if (LOG.isInfoEnabled()) {
            LOG.info("Webinterface kill request for jobs="
                   + Arrays.toString(jobs) + " by user "
                   + (user == null ? " anonymous (null)" : user )
                   + " REMOTE_ADDR=" + request.getRemoteAddr()
                   + " REMOTE_HOST=" + request.getRemoteHost());
          }

          for (String job : jobs) {
            final JobID jobId = JobID.forName(job);
            if (user != null) {
              UserGroupInformation ugi =
                UserGroupInformation.createRemoteUser(user);
              try {
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                  public Void run() throws IOException{

                    tracker.killJob(jobId);// checks job modify permission
                    return null;
                  }
                });
              } catch(AccessControlException e) {
                errMsg = errMsg.concat("<br />" + e.getMessage());
                notAuthorized = true;
                // We don't return right away so that we can try killing other
                // jobs that are requested to be killed.
                continue;
              }
            }
            else {// no authorization needed
              tracker.killJob(jobId);
            }
          }
          if (notAuthorized) {// user is not authorized to kill some/all of jobs
            errMsg = errMsg.concat(
              "<br /><hr><a href=\"jobtracker.jsp\">Go back to JobTracker</a><br />");
            setErrorAndForward(errMsg, request, response);
            return;
          }
        }
      }

    if (privateActionsAllowed(tracker.conf) && 
          request.getParameter("changeJobPriority") != null) {
        String[] jobs = request.getParameterValues("jobCheckBox");
        if (jobs != null) {
          final JobPriority jobPri = JobPriority.valueOf(request
              .getParameter("setJobPriority"));
          boolean notAuthorized = false;
          String errMsg = "User " + user
              + " failed to set priority for the following job(s)!<br /><br />";

          for (String job : jobs) {
            final JobID jobId = JobID.forName(job);
            if (user != null) {
              UserGroupInformation ugi = UserGroupInformation.
                  createRemoteUser(user);
              try {
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                  public Void run() throws IOException{

                    // checks job modify permission
                    tracker.setJobPriority(jobId, jobPri);
                    return null;
                  }
                });
              } catch(AccessControlException e) {
                errMsg = errMsg.concat("<br />" + e.getMessage());
                notAuthorized = true;
                // We don't return right away so that we can try operating on
                // other jobs.
                continue;
              }
            }
            else {// no authorization needed
              tracker.setJobPriority(jobId, jobPri);
            }
          }
          if (notAuthorized) {// user is not authorized to kill some/all of jobs
            errMsg = errMsg.concat(
              "<br /><hr><a href=\"jobtracker.jsp\">Go back to JobTracker</a><br />");
            setErrorAndForward(errMsg, request, response);
            return;
          }
        }
      }
  }

  private static long getCounterValue(Counters counters, Enum<?> name, long errValue) {
    if (counters != null) {
      try {
          return counters.findCounter(name).getValue();
      } catch (Counters.CountersExceededException cee) {
          LOG.warn("Counters limit exceeded calling findCounter in jobtracker.jsp.", cee);
      } catch (Throwable e) {
          LOG.warn("Exception/Error calling findCounter in jobtracker.jsp.", e);
      }
    }
    return errValue;
  }

  private static long getCounterValueOrZero(Counters counters, Enum<?> name) {
    return getCounterValue(counters, name, 0);
  }

  private static long getCounterValueOrNegativeOne(Counters counters, Enum<?> name) {
    return getCounterValue(counters, name, -1);
  }

  public static Collection<JobInProgress> sortByCPU(Collection<JobInProgress> job) {
    Comparator<JobInProgress> cpuComparator  =
      new Comparator<JobInProgress>() {
        public int compare(JobInProgress job1, JobInProgress job2) {
          long job1Cpu = 
            getCounterValueOrZero(job1.getMapCounters(), Task.Counter.CPU_MILLISECONDS) +
            getCounterValueOrZero(job1.getReduceCounters(), Task.Counter.CPU_MILLISECONDS);
          long job2Cpu = 
            getCounterValueOrZero(job2.getMapCounters(), Task.Counter.CPU_MILLISECONDS) +
            getCounterValueOrZero(job2.getReduceCounters(), Task.Counter.CPU_MILLISECONDS);
          return (job1Cpu > job2Cpu)? -1 : (job1Cpu == job2Cpu) ? 0 : 1;
        }
      };
    List <JobInProgress> jobList = new ArrayList<JobInProgress>(job);
    Collections.sort(jobList, cpuComparator);
    return jobList;
  }
  
  public static Collection<JobInProgress> sortByFinishTime(Collection<JobInProgress> job) {
    Comparator<JobInProgress> timeComparator  =
      new Comparator<JobInProgress>() {
        public int compare(JobInProgress job1, JobInProgress job2) {
          long job1FinishTime = job1.getFinishTime();
          long job2FinishTime = job2.getFinishTime();
          return (job1FinishTime > job2FinishTime)? -1 : (job1FinishTime == job2FinishTime)? 0: 1;
        }
      };
    List <JobInProgress> jobList = new ArrayList<JobInProgress>(job);
    Collections.sort(jobList, timeComparator);
    return jobList;
  }

  /**
   * Method used to generate the Job table for Job pages.
   * 
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @return
   * @throws IOException
   */
  public static String generateJobTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId, JobConf conf, final JobTracker tracker) throws IOException {

    boolean isModifiable = label.equals("Running") 
                                && privateActionsAllowed(conf);
    boolean isFailed = label.equals("Failed");
    boolean isCompleted = isFailed || label.equals("Completed");

    StringBuffer sb = new StringBuffer();
    
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
    if (jobs.size() > 0) {
      if (isModifiable) {
        sb.append("<form action=\"/jobtracker.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
      }
      sb.append("<thead>");
      if (isModifiable) {
        sb.append("<tr>");
        sb.append("<td><input type=\"Button\" onclick=\"selectAll()\" " +
        		"value=\"Select All\" id=\"checkEm\"></td>");
        sb.append("<td><nobr>");
        sb.append("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
          sb.append("<option"
              + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">")
              + prio + "</option>");
        }

        sb.append("</select>");
        sb.append("<input type=\"submit\" name=\"changeJobPriority\" " +
        		"value=\"Change\">");
        sb.append("</nobr></td>");
        sb.append("<td colspan=\"16\"><input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\"></td>");
        sb.append("</tr>");
      }
      sb.append("<tr>");

      if (isModifiable) {
        sb.append("<td>&nbsp;</td>");
      }
      sb.append("<th>");
      sb.append("<b>Jobid</b></th><th><b>Priority" +
				"</b></th><th><b>User</b></th>");
      sb.append("<th><b>Name</b></th>");
      sb.append("<th><b>Start Time</b></th>");
      if (!isCompleted) {
        sb.append("<th><b>Map % Complete</b></th>");
        sb.append("<th><b>Current Map Slots</b></th>");
        sb.append("<th><b>Failed MapAttempts</b></th>");
        sb.append("<th><b>MapAttempt Time Avg/Max</b></th>");
        sb.append("<th><b>Cumulative Map CPU</b></th>");
        sb.append("<th><b>Current Map PMem</b></th>");
        sb.append("<th><b>Reduce % Complete</b></th>");
        sb.append("<th><b>Current Reduce Slots</b></th>");
        sb.append("<th><b>FailedReduce Attempts</b></th>");
        sb.append("<th><b>ReduceAttempt Time Avg/Max</b></th>");
        sb.append("<th><b>Cumulative Reduce CPU</b></th>");
        sb.append("<th><b>Current Reduce PMem</b></th>");
      } else {
        sb.append("<th><b>Total Time</b></th>");
        sb.append("<th><b>Maps Launched</b></th>");
        sb.append("<th><b>Map Total</b></th>");
        sb.append("<th><b>Failed MapAttempts</b></th>");
        sb.append("<th><b>MapAttempt Time Avg/Max</b></th>");
        sb.append("<th><b>Cumulative Map CPU</b></th>");
        sb.append("<th><b>Cumulative Map PMem</b></th>");
        sb.append("<th><b>Reducers Launched</b></th>");
        sb.append("<th><b>Reduce Total</b></th>");
        sb.append("<th><b>Failed ReduceAttempts</b></th>");
        sb.append("<th><b>ReduceAttempt Time Avg/Max</b></th>");
        sb.append("<th><b>Cumulative Reduce CPU</b></th>");
        sb.append("<th><b>Cumulative Reduce PMem</b></th>");
      }
      sb.append("</tr>\n");
      sb.append("</thead><tbody>");
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = 0, desiredReduces = 0, runningMaps = 0, runningReduces = 0;
        long launchedMaps = 0, launchedReduces = 0;
        long mapCPU, mapPMem, reduceCPU, reducePMem;
        Counters mapCounters, reduceCounters, mapRunningCounters, reduceRunningCounters;
        long avgMapTaskTime, maxMapTaskTime;
        long avgReduceTaskTime, maxReduceTaskTime;
        int failedMaps, failedReduces;
        long jobTime = 0;

        avgMapTaskTime = job.avgMapTime();
        avgReduceTaskTime = job.avgReduceTime();
        maxMapTaskTime = job.maxMapTime();
        maxReduceTaskTime = job.maxReduceTime();
        failedMaps = job.failedMaps();
        failedReduces = job.failedReduces();

        mapCounters = job.getMapCounters();
        reduceCounters = job.getReduceCounters();

        if (!isCompleted) {
          runningMaps = job.runningMaps();
          runningReduces = job.runningReduces();
          mapRunningCounters = job.getRunningMapCounters();
          reduceRunningCounters = job.getRunningReduceCounters();
          mapPMem = getCounterValueOrNegativeOne(mapRunningCounters, Task.Counter.PHYSICAL_MEMORY_BYTES);
          reducePMem = getCounterValueOrNegativeOne(reduceRunningCounters, Task.Counter.PHYSICAL_MEMORY_BYTES);
        } else {
          Counters jobCounters = job.getJobCounters();
          desiredMaps = job.desiredMaps();
          desiredReduces = job.desiredReduces();
          launchedMaps = getCounterValueOrNegativeOne(jobCounters, JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
          launchedReduces = getCounterValueOrNegativeOne(jobCounters, JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
          mapPMem = getCounterValueOrNegativeOne(mapCounters, Task.Counter.PHYSICAL_MEMORY_BYTES);
          reducePMem = getCounterValueOrNegativeOne(reduceCounters, Task.Counter.PHYSICAL_MEMORY_BYTES);
          jobTime = job.getFinishTime() - job.getStartTime();
        }
        mapCPU = getCounterValueOrNegativeOne(mapCounters, Task.Counter.CPU_MILLISECONDS);
        reduceCPU = getCounterValueOrNegativeOne(reduceCounters, Task.Counter.CPU_MILLISECONDS);

        String mapCPUStr = (mapCPU >= 0) ? StringUtils.formatTime(mapCPU) : COUNTER_LIMIT_STR;
        String reduceCPUStr = (reduceCPU >= 0) ? StringUtils.formatTime(reduceCPU) : COUNTER_LIMIT_STR;
        String mapPMemStr = (mapPMem >= 0) ? StringUtils.byteDesc(mapPMem) : COUNTER_LIMIT_STR;
        String reducePMemStr = (reducePMem >= 0) ? StringUtils.byteDesc(reducePMem) : COUNTER_LIMIT_STR;
        String launchedMapsStr = (launchedMaps >= 0) ? "" + launchedMaps : COUNTER_LIMIT_STR;
        String launchedReducesStr = (launchedReduces >= 0) ? "" + launchedReduces : COUNTER_LIMIT_STR;

        String name = HtmlQuoting.quoteHtmlChars(profile.getJobName());
        String jobpri = job.getPriority().toString();
        if (isModifiable) {
          sb.append("<tr><td><input TYPE=\"checkbox\" " +
          		"onclick=\"checkButtonVerbage()\" " +
          		"name=\"jobCheckBox\" value="
                  + jobid + "></td>");
        } else {
          sb.append("<tr>");
        }

        sb.append("<td id=\"job_" + rowId
            + "\"><a href=\"jobdetails.jsp?jobid=" + jobid + "&refresh="
            + refresh + "\">" + jobid + "</a></td>" + "<td id=\"priority_"
            + rowId + "\">" + jobpri + "</td>" + "<td id=\"user_" + rowId
            + "\">" + HtmlQuoting.quoteHtmlChars(profile.getUser()) +
              "</td>" + "<td id=\"name_" + rowId
            + "\">" + ("".equals(name) ? "&nbsp;" : name) + "</td>" +
            "<td>" +  new Date(job.getStartTime()) + "</td>");
         if (!isCompleted) {
           sb.append(
               // map 
               "<td>" + StringUtils.formatPercent(status.mapProgress(), 2)
               + ServletUtil.percentageGraph(status.mapProgress() * 100, 80)
               // running maps
               + "</td><td>" + ((runningMaps > 0)
                ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "map" +
                "&pagenum=1" + "&state=running\">" + runningMaps + "</a>")
                : "0") + "</td><td>" +
               // failed maps
               ((failedMaps > 0) ? ("<a href=\"jobfailures.jsp?jobid=" + jobid +
                   "&kind=" + "map" + "&cause=failed\">" + failedMaps +
                   "</a>") : "0") + "</td><td>" +
               // completed maps, avg time, max time
               ((avgMapTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "map" +
               "&pagenum=1" + "&state=completed\">" + StringUtils.formatTime(avgMapTaskTime) + "</a>")
                : "0sec")
               + "/" 
               + ((maxMapTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "map" +
                   "&pagenum=1" + "&state=completed&timetaken=" + avgMapTaskTime +"\">" + 
                   StringUtils.formatTime(maxMapTaskTime) + "</a>")
                 : "0sec") + "</td><td>"
               + mapCPUStr + "</td><td>"
               + mapPMemStr + "</td>"
               // reduce
               + "<td>" + StringUtils.formatPercent(status.reduceProgress(), 2)
               + ServletUtil.percentageGraph(status.reduceProgress() * 100, 80)
               // running reduces
               + "</td><td>" + ((runningReduces > 0)
                 ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "reduce" +
                 "&pagenum=1" + "&state=running\">" + runningReduces + "</a>")
                 : "0") + "</td><td>" +
               // failed reduces
               ((failedReduces > 0) ? ("<a href=\"jobfailures.jsp?jobid=" + jobid +
                   "&kind=" + "reduce" + "&cause=failed\">" + failedReduces +
                   "</a>") : "0") + "</td><td>" +
               // completed reduces, avg time, max time
               ((avgReduceTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "reduce" +
               "&pagenum=1" + "&state=completed\">" + StringUtils.formatTime(avgReduceTaskTime) + "</a>") 
                : "0sec")
               + "/" 
               + ((maxReduceTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "reduce" +
                   "&pagenum=1" + "&state=completed&timetaken=" + avgReduceTaskTime +"\">" + 
                   StringUtils.formatTime(maxReduceTaskTime) + "</a>") 
                 : "0sec") + "</td><td>"
               + reduceCPUStr + "</td><td> "
               + reducePMemStr + "</td></tr>\n");
         } else {
           sb.append(
               // total time 
               "<td>" + StringUtils.formatTime(jobTime) + "</td>" +
               // map 
               "<td>" + launchedMapsStr + "</td><td>" + desiredMaps
               // failed maps
               + "</td><td>" + ((failedMaps > 0) ?
                ("<a href=\"jobfailures.jsp?jobid=" + jobid +
                 "&kind=" + "map" + "&cause=failed\">" + failedMaps +
                 "</a>") : "0") 
               + "</td><td>" +  
               // completed maps, avg time, max time
               ((avgMapTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "map" +
               "&pagenum=1" + "&state=completed\">" + StringUtils.formatTime(avgMapTaskTime) + "</a>") 
                : "0sec") 
               + "/" 
               + ((maxMapTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "map" +
                   "&pagenum=1" + "&state=completed&timetaken=" + avgMapTaskTime +"\">" + 
                   StringUtils.formatTime(maxMapTaskTime) + "</a>") 
                 : "0sec")
               + "</td><td>" + mapCPUStr + "</td><td>"
               + mapPMemStr + "</td>"
               // reduce
               + "<td>" + launchedReducesStr + "</td><td>" + desiredReduces
               // failed reduces
               + "</td><td>" + ((failedReduces > 0) ?
                ("<a href=\"jobfailures.jsp?jobid=" + jobid +
                 "&kind=" + "reduce" + "&cause=failed\">" + failedReduces +
                 "</a>") : "0")
               + "</td><td>" +
               // completed reduces, avg time, max time
               ((avgReduceTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "reduce" +
               "&pagenum=1" + "&state=completed\">" + StringUtils.formatTime(avgReduceTaskTime) + "</a>") 
                : "0sec") 
               + "/" 
               + ((maxReduceTaskTime > 0) ? ("<a href=\"jobtasks.jsp?jobid=" + jobid + "&type="+ "reduce" +
                   "&pagenum=1" + "&state=completed&timetaken=" + avgReduceTaskTime +"\">" + 
                   StringUtils.formatTime(maxReduceTaskTime) + "</a>") 
                 : "0sec")
               + "</td><td>" + reduceCPUStr + "</td><td>"
               + reducePMemStr + "</td>");
           sb.append("</tr>\n");
         }
      }
      sb.append("</tbody>");
      if (isModifiable) {
        sb.append("</form>\n");
      }
    } else {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      		"</td></tr>\n");
    }
    sb.append("</table>\n");
    
    return sb.toString();
  }


  @SuppressWarnings("unchecked")
  public static String generateRetiredJobTable(JobConf conf, JobTracker tracker, int rowId) 
    throws IOException {

    StringBuffer sb = new StringBuffer();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"sortable\">\n");

    Iterator<RetireJobInfo> iterator = 
      tracker.retireJobs.getAll().descendingIterator();
    if (!iterator.hasNext()) {
      sb.append("<tr><td align=\"center\" colspan=\"21\"><i>none</i>" +
      "</td></tr>\n");
    } else {
      sb.append("<tr>");
    
      sb.append("<th><b>Jobid</b></th>");
      sb.append("<th><b>Priority</b></th>");
      sb.append("<th><b>User</b></th>");
      sb.append("<th><b>Name</b></th>");
      sb.append("<th><b>State</b></th>");
      sb.append("<th><b>Start Time</b></th>");
      sb.append("<th><b>Total Time</b></th>");
      sb.append("<th><b>Maps Launched</b></th>");
      sb.append("<th><b>Map Total</b></th>");
      sb.append("<th><b>Failed MapAttempts</b></th>");
      sb.append("<th><b>MapAttempt Time Avg/Max</b></th>");
      sb.append("<th><b>Cumulative Map CPU</b></th>");
      sb.append("<th><b>Cumulative Map PMem</b></th>");
      sb.append("<th><b>Reducers Launched</b></th>");
      sb.append("<th><b>Reduce Total</b></th>");
      sb.append("<th><b>Failed ReduceAttempts</b></th>");
      sb.append("<th><b>ReduceAttempt Time Avg/Max</b></th>");
      sb.append("<th><b>Cumulative Reduce CPU</b></th>");
      sb.append("<th><b>Cumulative Reduce PMem</b></th>");
      sb.append("<th><b>Job Scheduling Information</b></th>");
      sb.append("<th><b>Diagnostic Info </b></th>");
      sb.append("</tr>\n");
      for (int i = 0; iterator.hasNext(); i++) {
        RetireJobInfo info = iterator.next();
        String historyFile = info.getHistoryFile();
        String historyFileUrl = null;
        if (historyFile != null && !historyFile.equals("")) {
          try {
            historyFileUrl = URLEncoder.encode(info.getHistoryFile(), "UTF-8");
          } catch (UnsupportedEncodingException e) {
            LOG.warn("Can't create history url ", e);
          }
        }
        long jobTime = info.finishTime - info.status.getStartTime();
        long launchedMaps = getCounterValueOrNegativeOne(info.counters, JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
        long launchedReduces = getCounterValueOrNegativeOne(info.counters, JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
        String launchedMapsStr = (launchedMaps >= 0) ? "" + launchedMaps : COUNTER_LIMIT_STR;
        String launchedReducesStr = (launchedReduces >= 0) ? "" + launchedReduces : COUNTER_LIMIT_STR;
        String diagInfoStr = HtmlQuoting.quoteHtmlChars(info.status.getFailureInfo());
        if (info.counters.size() >= Counters.MAX_COUNTER_LIMIT) {
          if (diagInfoStr.equals("NA")) {
            diagInfoStr = "Reached limit on number of counters.";
          } else {
            diagInfoStr += "<br>Reached limit on number of counters.";
          }
        }

        sb.append("<tr>");
        sb.append(
            "<td id=\"job_" + rowId + "\">" +             
            (historyFileUrl == null ? "" :
              "<a href=\"jobdetailshistory.jsp?logFile=" + historyFileUrl + "\">") +
            info.status.getJobId() + "</a></td>" +         
            "<td id=\"priority_" + rowId + "\">" +
            info.status.getJobPriority().toString() + "</td>" +
            "<td id=\"user_" + rowId + "\">" +
            HtmlQuoting.quoteHtmlChars(info.profile.getUser()) + "</td>" +
            "<td id=\"name_" + rowId + "\">" +
            HtmlQuoting.quoteHtmlChars(info.profile.getJobName()) + "</td>" +
            "<td>" + JobStatus.getJobRunState(info.status.getRunState()) + "</td>" +
            "<td>" + new Date(info.status.getStartTime()) + "</td>" +
            "<td>" + StringUtils.formatTime(jobTime) + "</td>" +
            // map 
            "<td>" + launchedMapsStr + "</td><td>" + info.desiredMaps
            // failed maps history file
            + "</td><td>" + ((info.failedMaps > 0) ?
                ("<a href=\"jobtaskshistory.jsp?logFile=" +  historyFileUrl +
                 "&taskType=" + JobHistory.Values.MAP.name() + 
                 "&status=" + JobHistory.Values.FAILED + "\">" +
                 info.failedMaps + "</a>") : "0")
            + "</td><td>" +
               // completed maps, avg time, max time
               ((info.avgMapTaskTime > 0) ?
                ("<a href=\"jobtaskshistory.jsp?logFile=" + historyFileUrl +
                 "&taskType=" + JobHistory.Values.MAP.name() +
                 "&status=" + JobHistory.Values.SUCCESS + "\">" +
                 StringUtils.formatTime(info.avgMapTaskTime) + "</a>") 
                : "0sec")
               + "/" 
               + ((info.maxMapTaskTime > 0) ?
                   ("<a href=\"jobtaskshistory.jsp?logFile=" + historyFileUrl +
                    "&taskType=" + JobHistory.Values.MAP.name() +                                                
                    "&status=" + JobHistory.Values.SUCCESS +
                    "&timetaken=" + info.avgMapTaskTime + "\">" + 
                    StringUtils.formatTime(info.maxMapTaskTime) + "</a>")
                 : "0sec")
             + "</td><td>" + StringUtils.formatTime(info.mapCPU) + "</td><td>"
             + StringUtils.byteDesc(info.mapPMem) + "</td>"
            // reduce
            + "<td>" + launchedReducesStr + "</td><td>" + info.desiredReduces
            // failed reducer history file
            + "</td><td>" + ((info.failedReduces > 0) ?
                ("<a href=\"jobtaskshistory.jsp?logFile=" +  historyFileUrl +
                 "&taskType=" + JobHistory.Values.REDUCE.name() + 
                 "&status=" + JobHistory.Values.FAILED + "\">" +
                 info.failedReduces + "</a>") : "0")
            + "</td><td>" +
               // completed reducers, avg time, max time
               ((info.avgReduceTaskTime > 0) ?
                ("<a href=\"jobtaskshistory.jsp?logFile=" + historyFileUrl +
                 "&taskType=" + JobHistory.Values.REDUCE.name() +
                 "&status=" + JobHistory.Values.SUCCESS + "\">" +
                 StringUtils.formatTime(info.avgReduceTaskTime) + "</a>") 
                : "0sec")
               + "/" 
               + ((info.maxReduceTaskTime > 0) ?
                   ("<a href=\"jobtaskshistory.jsp?logFile=" + historyFileUrl +
                    "&taskType=" + JobHistory.Values.REDUCE.name() +                                                
                    "&status=" + JobHistory.Values.SUCCESS +
                    "&timetaken=" + info.avgReduceTaskTime + "\">" + 
                    StringUtils.formatTime(info.maxReduceTaskTime) + "</a>")
                 : "0sec")
             + "</td><td>" + StringUtils.formatTime(info.reduceCPU) + "</td><td>"
             + StringUtils.byteDesc(info.reducePMem) + "</td>"
           // scheduling and debug
            + "<td>" + HtmlQuoting.quoteHtmlChars(info.status.getSchedulingInfo()) + "</td>" 
            + "<td>" + diagInfoStr
            +  "</td></tr>\n");
        rowId++;
      }
    }
    sb.append("</table>\n");
    return sb.toString();
  }

  static Path getJobConfFilePath(Path logFile) {
    String[] jobDetails = logFile.getName().split("_");
    String jobId = getJobID(logFile.getName());
    String jobUniqueString =
        jobDetails[0] + "_" + jobDetails[1] + "_" + jobId;
    Path logDir = logFile.getParent();
    Path jobFilePath = new Path(logDir, jobUniqueString + "_conf.xml");
    return jobFilePath;
  }

  /**
   * Read a job-history log file and construct the corresponding {@link JobInfo}
   * . Also cache the {@link JobInfo} for quick serving further requests.
   * 
   * @param logFile
   * @param fs
   * @param jobTracker
   * @return JobInfo
   * @throws IOException
   */
  static JobInfo getJobInfo(Path logFile, FileSystem fs,
      JobTracker jobTracker, String user) throws IOException {
    String jobid = getJobID(logFile.getName());
    JobInfo jobInfo = null;
    synchronized(jobHistoryCache) {
      jobInfo = jobHistoryCache.remove(jobid);
      if (jobInfo == null) {
        jobInfo = new JobHistory.JobInfo(jobid);
        LOG.info("Loading Job History file "+jobid + ".   Cache size is " +
            jobHistoryCache.size());
        DefaultJobHistoryParser.parseJobTasks(logFile.toUri().getPath(),
            jobInfo, fs);
      }
      jobHistoryCache.put(jobid, jobInfo);
      int CACHE_SIZE = 
        jobTracker.conf.getInt("mapred.jobtracker.jobhistory.lru.cache.size", 5);
      if (jobHistoryCache.size() > CACHE_SIZE) {
        Iterator<Map.Entry<String, JobInfo>> it = 
          jobHistoryCache.entrySet().iterator();
        String removeJobId = it.next().getKey();
        it.remove();
        LOG.info("Job History file removed form cache "+removeJobId);
      }
    }

    UserGroupInformation currentUser;
    if (user == null) {
      currentUser = UserGroupInformation.getCurrentUser();
    } else {
      currentUser = UserGroupInformation.createRemoteUser(user);
    }

    // Authorize the user for view access of this job
    jobTracker.getACLsManager().checkAccess(jobid, currentUser,
        jobInfo.getJobQueue(), Operation.VIEW_JOB_DETAILS,
        jobInfo.get(Keys.USER), jobInfo.getJobACLs().get(JobACL.VIEW_JOB));

    return jobInfo;
  }

  /**
   * Check the access for users to view job-history pages.
   * 
   * @param request
   * @param response
   * @param jobTracker
   * @param fs
   * @param logFile
   * @return the job if authorization is disabled or if the authorization checks
   *         pass. Otherwise return null.
   * @throws IOException
   * @throws InterruptedException
   * @throws ServletException
   */
  static JobInfo checkAccessAndGetJobInfo(HttpServletRequest request,
      HttpServletResponse response, final JobTracker jobTracker,
      final FileSystem fs, final Path logFile) throws IOException,
      InterruptedException, ServletException {
    String jobid = getJobID(logFile.getName());
    String user = request.getRemoteUser();
    JobInfo job = null;
    if (user != null) {
      try {
        job = JSPUtil.getJobInfo(logFile, fs, jobTracker, user);
      } catch (AccessControlException e) {
        String errMsg =
            String.format(
                "User %s failed to view %s!<br /><br />%s"
                    + "<hr>"
                    + "<a href=\"jobhistory.jsp\">Go back to JobHistory</a><br />"
                    + "<a href=\"jobtracker.jsp\">Go back to JobTracker</a>",
                user, jobid, e.getMessage());
        JSPUtil.setErrorAndForward(errMsg, request, response);
        return null;
      }
    } else {
      // no authorization needed
      job = JSPUtil.getJobInfo(logFile, fs, jobTracker, null);
    }
    return job;
  }

  static String getJobID(String historyFileName) {
    String[] jobDetails = historyFileName.split("_");
    return jobDetails[2] + "_" + jobDetails[3] + "_" + jobDetails[4];
  }

  static String getUserName(String historyFileName) {
    String[] jobDetails = historyFileName.split("_");
    return jobDetails[5];
  }

  static String getJobName(String historyFileName) {
    String[] jobDetails = historyFileName.split("_");
    return jobDetails[6];
  }

  /**
   * Nicely print the Job-ACLs
   * @param tracker
   * @param jobAcls
   * @param out
   * @throws IOException
   */
  static void printJobACLs(JobTracker tracker,
      Map<JobACL, AccessControlList> jobAcls, JspWriter out)
      throws IOException {
    if (tracker.areACLsEnabled()) {
      // Display job-view-acls and job-modify-acls configured for this job
      out.print("<b>Job-ACLs:</b><br />");
      for (JobACL aclName : JobACL.values()) {
        String aclConfigName = aclName.getAclName();
        AccessControlList aclConfigured = jobAcls.get(aclName);
        if (aclConfigured != null) {
          String aclStr = aclConfigured.toString();
          out.print("&nbsp;&nbsp;&nbsp;&nbsp;" + aclConfigName + ": "
              + aclStr + "<br />");
        }
      }
    }
    else {
      out.print("<b>Job-ACLs: " + new AccessControlList("*").toString()
          + "</b><br />");
    }
  }

  static boolean privateActionsAllowed(JobConf conf) {
    return conf.getBoolean(PRIVATE_ACTIONS_KEY, false);
  }
}
