/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.YarnAppUtil;

/**
 * Handler to respond to a newly registered application with ResourceManager.
 * It creates a MapR ticket for the application and stores it in MapRFS.
 * The NodeManager will localize this ticket and make it available to the
 * tasks.
 */
public class MapRTicketGenerator {
  private static final Log LOG = LogFactory.getLog(MapRTicketGenerator.class);

  private static final String Security = "com.mapr.security.Security";

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
  private void generateMapRLoginTicket(ApplicationId appId, String username,
      Configuration conf) throws IOException {

    String appIdStr = appId.toString();
    FileSystem fs = FileSystem.get(conf);
    Path appStagingDir = YarnAppUtil.getRMStagingDir(appIdStr, fs, conf);
    Path existingTicketPath = YarnAppUtil.getMapRTicketPath(appStagingDir);

    FSDataInputStream fsin = null;
    if (fs.isFile(existingTicketPath)) {
      fsin = fs.open(existingTicketPath);
    }

    // Create app system dir
    Path appSystemDir = YarnAppUtil.getRMSystemDir(appIdStr, fs, conf);
    FileSystem.mkdirs(fs, appSystemDir, YarnAppUtil.APP_DIR_PERMISSION);

    Path ticketPath = YarnAppUtil.getMapRTicketPath(appSystemDir);
    // Create the path on MapRFS for the ticket. If the path exists,
    // it will be overwritten.
    FSDataOutputStream outTicket =
      FileSystem.create(fs, ticketPath,
          new FsPermission(YarnAppUtil.APP_FILE_PERMISSION));

    long expiration = System.currentTimeMillis() +
      Long.parseLong(conf.get(YarnDefaultProperties.MAPR_TICKET_EXPIRY));

    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(Security);
      if (fsin != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cloning MapR ticket for app: " + appId
              + " user: " + username + " at " + ticketPath);
        }
        Method cloneAndGenerateTicketFile = klass.getDeclaredMethod("CloneAndGenerateTicketFile",
            FSDataInputStream.class, long.class, FSDataOutputStream.class);
        cloneAndGenerateTicketFile.invoke(null, fsin, expiration, outTicket);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Generating MapR ticket for app: " + appId
              + " user: " + username + " at " + ticketPath);
        }
        Method mergeAndGenerateTicketFile = klass.getDeclaredMethod("MergeAndGenerateTicketFile",
            FSDataInputStream.class, String.class, long.class, FSDataOutputStream.class);
        mergeAndGenerateTicketFile.invoke(null, fsin, username, expiration, outTicket);
      }
    } catch (Throwable t) {
      throw new IOException("Security Ticket for app: " + appId
          + " and user: " + username + " failed", t);
    } finally {
      if (fsin != null) {
        fsin.close();
      }
      outTicket.close();

      // Remove the staged ticket dir as the ticket has been merged and
      // uploaded in a new location and is therefore no longer needed.
      // COMSECURE-117: The staging directory is no longer removed so that
      // tickets for other clusters will be available.
      if (LOG.isDebugEnabled()) {
        LOG.error("Retained staged ticket dir from MFS: " + appStagingDir);
      }
    }
  }
}
