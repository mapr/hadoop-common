/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.YarnAppUtil;

import java.io.IOException;

/**
 * Handler to respond to a newly registered application with ResourceManager.
 * It creates a MapR ticket for the application and stores it in MapRFS.
 * The NodeManager will localize this ticket and make it available to the
 * tasks.
 */
public abstract class AbstractMapRTicketGenerator {
  private static final Log LOG = LogFactory.getLog(AbstractMapRTicketGenerator.class);

  public void generateToken(ApplicationSubmissionContext appCtx, String username,
      Configuration conf) {

    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    ApplicationId appId = appCtx.getApplicationId();
    try {
      generateMapRLoginTicket(appId, username, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void removeToken(ApplicationId appId, Configuration conf) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    try {
      FileSystem fs = FileSystem.get(conf);
      Path path = YarnAppUtil.getRMSystemDir(appId.toString(), fs, conf);
      fs.delete(path, true);

      if (LOG.isDebugEnabled()) {
        LOG.error("Removed system ticket dir from MFS: " + path);
      }

      Path appStagingDir = YarnAppUtil.getRMStagingDir(appId.toString(), fs, conf);
      fs.delete(appStagingDir, true);

      if (LOG.isDebugEnabled()) {
        LOG.error("Removed staging ticket dir from MFS: " + path);
      }

    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * Generates MapR Ticket for current cluster and write it to system directory
   * of the application. If there is an existing ticket in the staging directory,
   * it is merged with the newly created ticket. The existing ticket is needed
   * as it could be for a different cluster.
   *
   * @param appId application id
   * @param username username
   * @param conf configuration
   *
   * @throws IOException if ticket generation fails
   */
   public abstract void generateMapRLoginTicket(ApplicationId appId, String username,
                                                  Configuration conf) throws IOException;
}
