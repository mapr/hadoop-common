package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.TaskStatus.State;

public class DescribeTasksServlet extends AbstractJsonRPCServlet<DescribeTasksServlet.TaskDescription, DescribeTasksServlet.RequestContext> {

  private static final Log  LOG              = LogFactory.getLog(DescribeTasksServlet.class.getName());
  private static final long serialVersionUID = -1365683739392460020L;
  
  static public class TaskDescription {
    public List<TaskAttemptDescription> taskAttempts;
    public String jobId;
    public String taskId;
    public String taskType;
    public String progress;
    public long startTime;
    public long finishTime;

    TaskDescription() {
      taskAttempts = new ArrayList<TaskAttemptDescription>();
    }
  }

  static public class TaskAttemptDescription {
    public String taskAttemptId;
    public String taskAttemptState;
    public long   startTime;
    public long   finishTime;
    public String phase;
    public float progress;
    public long shuffleFinishTime;
    public long sortFinishTime;

    public TaskAttemptDescription() {
    }
  }

  static class RequestContext extends AbstractJsonRPCServlet.AbstractRequestContext<DescribeTasksServlet.TaskDescription> {

    public RequestContext(
      HttpServletRequest servletRequest,
      HttpServletResponse servletResponse,
      JobTracker jobTracker
    )
    {
      super(servletRequest, servletResponse, jobTracker);
    }

    public String constructRequest() {
      String request = servletRequest.getParameter("jobId");
      validateRequest(request);
      return request;
    }

    private void validateRequest(String request) {
      if ( request == null ) {
        throw new AbstractJsonRPCServlet.ErrorException("Expected parameter jobId", 500);
      }
    }

    public void performRequest() {
      String request = constructRequest();
      JobInProgress job = jobTracker.getJob(JobID.forName(request));
      if ( job == null ) {
        throw new AbstractJsonRPCServlet.ErrorException("job does not exist", 500);
      }
      sendTastAttempts(request, job.getTasks(TaskType.MAP));
      sendTastAttempts(request, job.getTasks(TaskType.REDUCE));
    }

    private void sendTastAttempts(String request, TaskInProgress[] tasks) {
      long taskStartTime = 0;
      long taskFinishTime = 0;
      for(TaskInProgress task: tasks) {
        TaskDescription taskDescription = new TaskDescription();
        taskDescription.jobId = request;
        taskDescription.taskId = task.getTIPId().toString();
        taskDescription.taskType = task.isMapTask() ? "map" : "reduce";
        taskDescription.progress = String.valueOf(task.getProgress());
        TaskStatus[] statuses = task.getTaskStatuses();
        for(TaskStatus status: statuses) {
          State taskState = status.getRunState();
          TaskAttemptDescription taskAttemptDescription = new TaskAttemptDescription();
          taskAttemptDescription.taskAttemptId = status.getTaskID().toString();
          taskAttemptDescription.taskAttemptState = taskState.toString();
          taskAttemptDescription.startTime = status.getStartTime();
          taskAttemptDescription.finishTime = status.getFinishTime();
          taskAttemptDescription.progress = status.getProgress();
          if ( ! task.isMapTask() ) {
            taskAttemptDescription.shuffleFinishTime = status.getShuffleFinishTime();
            taskAttemptDescription.sortFinishTime = status.getSortFinishTime();
          }
          taskAttemptDescription.phase = status.getPhase().toString();
          taskDescription.taskAttempts.add(taskAttemptDescription);
        }
        taskDescription.startTime = taskStartTime;
        taskDescription.finishTime = taskFinishTime;
        sendResponse(taskDescription);
      }
    }

  }

   @Override
  public Log getLog() {
    return LOG;
  }

  @Override
  public RequestContext newRequestContext(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
      JobTracker jobTracker) {
    return new RequestContext(servletRequest, servletResponse, jobTracker);
  }
}
