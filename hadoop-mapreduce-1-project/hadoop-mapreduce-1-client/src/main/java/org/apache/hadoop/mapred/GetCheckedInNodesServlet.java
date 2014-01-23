package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetCheckedInNodesServlet extends
    AbstractJsonRPCServlet<GetCheckedInNodesServlet.HostStatus, GetCheckedInNodesServlet.RequestContext> {

  private static final long serialVersionUID = -4132060592752755835L;
  private static final Log LOG = LogFactory.getLog(GetCheckedInNodesServlet.class.getName());

  public static class HostStatus {
    public List<String> checkedInHostPrivateDNSNames;

    public HostStatus() {
      checkedInHostPrivateDNSNames = new ArrayList<String>();
    }
  }
  
  static class RequestContext extends AbstractJsonRPCServlet.AbstractRequestContext<HostStatus> {

    public RequestContext(HttpServletRequest servletRequest, HttpServletResponse servletResponse, JobTracker jobTracker) {
      super(servletRequest, servletResponse, jobTracker);
    }

    public void performRequest() {

      Collection<TaskTrackerStatus> allTrackerStatus = jobTracker.taskTrackers();

      HashSet<String> checkedInHosts = new HashSet<String>();
      for (TaskTrackerStatus trackerStatus : allTrackerStatus) {
        checkedInHosts.add(trackerStatus.getHost());
      }

      HostStatus hostStatus = new HostStatus();
      hostStatus.checkedInHostPrivateDNSNames = new ArrayList<String>(checkedInHosts);
      sendResponse(hostStatus);
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
