/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.DefaultYarnConfiguration;

import java.lang.reflect.InvocationTargetException;

/**
 * Manages MapR ticket needed for running the application.
 */
public class MapRTicketManager implements ExternalTokenManager {
  private static final Logger LOG = LoggerFactory.getLogger(MapRTicketManager.class);

  private MapRTicketUploader uploader;
  private AbstractMapRTicketGenerator generator;
  private static final String MapRTicketGenerator = "com.mapr.security.MapRTicketGenerator";

  public MapRTicketManager() {
    uploader = new MapRTicketUploader();
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(MapRTicketGenerator);
      generator = (AbstractMapRTicketGenerator) klass.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException err) {
      LOG.error("Could not find MapRTicketGenerator class at classpath");
      err.printStackTrace();
    } catch ( NoSuchMethodException | IllegalAccessException |
            InstantiationException | InvocationTargetException e) {
      LOG.error("Could not instantiate MapRTicketGenerator");
      e.printStackTrace();
    }

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
