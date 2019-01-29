/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;
import org.apache.hadoop.yarn.security.ExternalTokenManager;

import org.apache.hadoop.yarn.server.nodemanager.security.MapRTicketUploader;

/**
 * Manages MapR ticket needed for running the application.
 */
public class MapRTicketManager implements ExternalTokenManager {
  private MapRTicketUploader uploader;
  private MapRTicketGenerator generator;

  public MapRTicketManager() {
    uploader = new MapRTicketUploader();
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
