package org.apache.hadoop.mapred;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class AbstractJsonRPCServlet<ReturnType, RequestContextType extends RequestContextInterface<ReturnType>> extends HttpServlet {
  
  private static final long serialVersionUID = -507112969847004059L;
  
  public static class ErrorException extends RuntimeException {
    private static final long serialVersionUID = -48469853031801025L;
    String errorMessage;
    int errorCode;
    
    public ErrorException(String errorMessage, int errorCode) {
      this.errorMessage = errorMessage;
      this.errorCode = errorCode;
    }
    
    public Object getErrorMessage() {
      return this.errorMessage;
    }
    
    public int getErrorCode() {
      return this.errorCode;
    }
  }

  AbstractJsonRPCServlet() {
  }
  
  static abstract public class AbstractRequestContext<ReturnType> implements RequestContextInterface<ReturnType> {
    HttpServletRequest  servletRequest;
    HttpServletResponse servletResponse;
    JobTracker          jobTracker;
    Gson                gson;
    
    public AbstractRequestContext (
        HttpServletRequest servletRequest,
        HttpServletResponse servletResponse,
        JobTracker jobTracker
    ) {
      super();
      this.servletRequest = servletRequest;
      this.servletResponse = servletResponse;
      this.jobTracker = jobTracker;
      this.gson = createJsonBuilder();
    }

    public void sendResponse(ReturnType response) {
      try {
        servletResponse.getWriter().println(gson.toJson(response));
      } catch (IOException e) {
        throw new RuntimeException("Error writing to result to output stream");
      }
    }
  }
  
  static Gson createJsonBuilder() {
    return new GsonBuilder().disableHtmlEscaping().create();
  }
  
  public void sendResponse(HttpServletResponse servletResponse, int statusCode, String response) {
      servletResponse.setStatus(statusCode);
      try {
        servletResponse.getWriter().println(createJsonBuilder().toJson(response));
      } catch (IOException e) {
        getLog().error("Error sending response", e);
      }
  }
  
  abstract public RequestContextType newRequestContext(HttpServletRequest servletRequest, HttpServletResponse servletResponse, JobTracker jobTracker);
  abstract public Log getLog();

  @Override
  public void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
    throws ServletException, IOException {
    {
      try {
        RequestContextType requestContext = newRequestContext(servletRequest, servletResponse,
            (JobTracker) getServletContext().getAttribute("job.tracker"));
        servletResponse.setContentType("text/json");
        servletResponse.setStatus(200);
        requestContext.performRequest();
      }
      catch(ErrorException e) {
        sendResponse(servletResponse, e.getErrorCode(), e.getMessage());
      }
      catch(Exception e) {
        getLog().error("Error processing request", e);
        sendResponse(servletResponse, 500, "Error processing request");
      }
    }    
  }
}
