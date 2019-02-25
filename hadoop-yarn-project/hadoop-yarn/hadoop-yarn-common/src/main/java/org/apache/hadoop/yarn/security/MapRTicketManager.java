/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;

/**
 * Manages MapR ticket needed for running the application.
 */
public class MapRTicketManager implements ExternalTokenManager {
  private AbstractMapRTicketUploader uploader;
  private MapRTicketGenerator generator;

  public MapRTicketManager() {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass("com.mapr.hadoop.yarn.security.MapRTicketUploader");
      uploader = (AbstractMapRTicketUploader) klass.newInstance();
    } catch (ClassNotFoundException err) {
      err.printStackTrace();
    } catch (InstantiationException | IllegalAccessException err) {
      err.printStackTrace();
    }
    generator = new MapRTicketGenerator();
  }

  @Override
  public void uploadTokenToDistributedCache(ApplicationId appId) {
    uploader.uploadToken(appId, DefaultYarnConfiguration.get());
  }

  @Override
  public void generateToken(ApplicationSubmissionContext appCtx, String username,
      Configuration conf) {

    generator.generateToken(appCtx, username, conf);
  }

  @Override
  public void removeToken(ApplicationId appId, Configuration conf) {
    generator.removeToken(appId, conf);
  }
}
