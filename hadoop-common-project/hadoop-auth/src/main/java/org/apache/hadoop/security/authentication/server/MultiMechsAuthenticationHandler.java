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

package org.apache.hadoop.security.authentication.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.log4j.Logger;

public class MultiMechsAuthenticationHandler implements AuthenticationHandler {

  private static final Logger LOG = Logger.getLogger(MultiMechsAuthenticationHandler.class);
  private static final String MTYPE = "multiauth";

  // We can later do may be factory based one class registration
  public static enum AuthHandlerEnum {
    BASIC {
      public String getType() {
        return "basic";
      }
      
      public String getMyClassName() {
        return "com.mapr.security.maprauth.BasicAuthHandler";
      }
      public int getOrder() {
        return 2;
      }
    },
    KERBEROS {
      public String getType() {
        return "kerberos";
      }
      public String getMyClassName() {
        return "org.apache.hadoop.security.authentication.server.KerberosAuthHandler";
      }
      public int getOrder() {
        return 1;
      }
    },
    MAPRAUTH {
      public String getType() {
        return "maprauth";
      }
      public String getMyClassName() {
        return "com.mapr.security.maprauth.MaprAuthenticationHandler";
      }
      public int getOrder() {
        return 0;
      }
    };
    
    public String getType() {
      return null;
    }
    
    public String getMyClassName() {
      return null;
    }
    
    public int getOrder() {
      return -1;
    }

    public static Class<? extends MultiMechsAuthenticationHandler> getClassFromType(String type) {
      for ( AuthHandlerEnum value : AuthHandlerEnum.values()) {
        if ( value.getType().equalsIgnoreCase(type)) {
          try {
            Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(value.getMyClassName());
            return  (Class<? extends MultiMechsAuthenticationHandler>) klass;
          } catch (ClassNotFoundException ex) {
            return null;
          } 
        }
      }
      return null;
    }
    
    /**
     * Since browsers pick auth mechanisms based on the order of the headers
     * we need to send headers ordered from Strongest to Weakest
     * @return
     */
    public static AuthHandlerEnum [] getOrderedArray() {
      AuthHandlerEnum [] retList = new AuthHandlerEnum[AuthHandlerEnum.values().length];
      for ( AuthHandlerEnum value : AuthHandlerEnum.values() ) {
        retList[value.getOrder()] = value;
      }
      return retList;
    }
  }
  
  private List<MultiMechsAuthenticationHandler> children = new ArrayList<MultiMechsAuthenticationHandler>();
  
  @Override
  public void init(Properties config) throws ServletException {
    int i = 0;
    while (true ) {
      String type = config.getProperty("type" + i);
      if ( type != null ) {
        // try to get class from type
        Class<? extends MultiMechsAuthenticationHandler> subClass = 
          AuthHandlerEnum.getClassFromType(type);
        if ( subClass != null ) {
          try {
            MultiMechsAuthenticationHandler child = subClass.newInstance();
            child.init(config);
            children.add(child);
          } catch (InstantiationException ex) {
            LOG.error("Unable to instantiate Authenticator Subclass of type: " + type + ". Will skip it", ex);
          } catch (IllegalAccessException ex) {
            LOG.error("Unable to init AuthenticationHandler Subclass of type: " + type + ". Will skip it", ex);
          } catch(Throwable t) {
            LOG.warn("Unable to init AuthenticationHandler Subclass " + subClass.getName() + 
                " that is used for authentication type " + type + ". Will skip it. " +
                "If no " + type + " configuration was intended no further action is needed" +
                " otherwise turn on DEBUG to see full exception trace");
            if ( LOG.isDebugEnabled() ) {
              LOG.debug("Full stacktrace", t);
            }
          }
        } else {
          LOG.error("No implementation found for type: " + type +
              ". Corresponding class should be registered with AuthHandlerEnum");
        }
        i++;
      } else {
        break;
      }
    }
    if ( children.isEmpty() ) {
      if ( i == 0 ) {
        // nothing was specified. Use all in enum
        for (AuthHandlerEnum value : AuthHandlerEnum.getOrderedArray()) {
          config.setProperty("type"+ value.getOrder(), value.getType());
        }
        init(config);
      } else {
        // there was some exception before
        throw new ServletException("Can not proceeds with initializing Authenticators, since " +
        		"no concrete implementations for AuthenticationHandler are found");
      }
    }
  }

  /**
   * Authenticate main method: 
   * In step 1. it will get headers from all children and return all of them ordered from strongest to weakest
   * In step 2. based on the client choice it will pick authentication mechanism and does authentication based on that one
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, final HttpServletResponse response)
    throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    if ( authorization != null ) {
      // since number of children is 1 or 2 or may be 3 no point creating map
      for ( MultiMechsAuthenticationHandler child : children ) {
        MultiMechsAuthenticationHandler result = child.getAuthBasedEntity(authorization);
        if ( result != null ) {
          // do either one - depending on the child
          return result.postauthenticate(request, response);          
        }
      }
      // if we are here - it is an unknown auth header
      LOG.error("Unknown Authorization: " + authorization + " please check your config files settings");
      throw new AuthenticationException("Unknown Authorization: " + authorization + " please check your config files settings");
    } else {
      // do all
      for ( MultiMechsAuthenticationHandler child : children ) {
        child.addHeader(response);
      }
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
    return token;
  }

  /**
   * Children must override this method with supplying corresponding header
   * 
   * @param response
   */
  protected void addHeader(HttpServletResponse response) {
    throw new UnsupportedOperationException("Each child of this class has to implement \'addHeader\' method");
  }

  /**
   * Children must override this method to perform real authentication based on the information received
   * in the request header
   * @param request
   * @param response
   * @return
   * @throws IOException
   * @throws AuthenticationException
   */
  protected AuthenticationToken postauthenticate(HttpServletRequest request, 
      final HttpServletResponse response)
  throws IOException, AuthenticationException {
    throw new UnsupportedOperationException("Each child of this class has to implement \'postauthenticate\' method");
  }
  
  @Override
  public void destroy() {
    for ( MultiMechsAuthenticationHandler child : children ) {
      child.destroy();
    }
  }

  /**
   * Children must override this method to essentially return "this" based on the request header
   * or null if request header is not suitable to a given auth mechanism
   * @param authorization
   * @return
   */
  protected MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    throw new UnsupportedOperationException("Each child of this class has to implement \'getAuthBasedEntity\' method");
  }

  @Override
  public String getType() {
    return MTYPE;
  }

  /**
   * Returns true by default.
   * Sub classes can override this method if they need to do any operations
   * such as get/renew/cancel delegation tokens.
   */
  @Override
  public boolean managementOperation(AuthenticationToken token,
                                     HttpServletRequest request,
                                     HttpServletResponse response)
    throws IOException, AuthenticationException {

    return true;
  }
}
