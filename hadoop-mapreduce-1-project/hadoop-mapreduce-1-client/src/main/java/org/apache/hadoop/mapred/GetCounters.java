package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetCounters extends AbstractJsonRPCServlet<GetCounters.HostCounters, GetCounters.RequestContext> {

  private static final long serialVersionUID = 2940967810803245588L;
  private static final Log LOG = LogFactory.getLog(GetCounters.class.getName());
  
  public static class HostCounters {
    public HashMap<String, Number> counterMap;

    public HostCounters() {
      counterMap = new HashMap<String, Number>();
    }
  }
  
  static class RequestContext extends AbstractJsonRPCServlet.AbstractRequestContext<HostCounters> {

    public RequestContext(HttpServletRequest servletRequest, HttpServletResponse servletResponse, JobTracker jobTracker) {
      super(servletRequest, servletResponse, jobTracker);
    }

    public void performRequest() {
      FetchCounters fetchCounters = new FetchCounters(jobTracker);
      
      HostCounters hostStatus = new HostCounters();
      hostStatus.counterMap = new HashMap<String, Number>();
      hostStatus.counterMap = fetchCounters.getCounterMap();
      sendResponse(hostStatus);
    }
  }
  
  @Override
  public RequestContext newRequestContext(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
      JobTracker jobTracker) {
    return new RequestContext(servletRequest, servletResponse, jobTracker);
  }

  @Override
  public Log getLog() {
    return LOG;
  }

}
