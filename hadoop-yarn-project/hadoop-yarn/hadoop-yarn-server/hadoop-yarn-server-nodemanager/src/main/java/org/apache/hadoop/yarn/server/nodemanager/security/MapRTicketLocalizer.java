/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.server.nodemanager.security;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.ExternalTokenLocalizer;

import org.apache.hadoop.yarn.server.nodemanager.util.YarnAppUtil;

/**
 * Localizer for MapR ticket. It expects the ticket to be already present in
 * MapRFS. If not, it will result in <code>YarnRuntimeException</code>.
 *
 * <p>
 * Note: Only a single instance of this class should be maintained per
 * NodeManager process since some global state information is maintained here.
 */
public class MapRTicketLocalizer implements ExternalTokenLocalizer {
  private static final Log LOG = LogFactory.getLog(MapRTicketLocalizer.class);

  /**
   * Cache access key per application to synchronize localization by multiple
   * containers belonging to the same application. This is to avoid localizing
   * the ticket multiple times.
   *
   * AtomicBoolean is used as the access key since we want to maintain state.
   * A true value implies the ticket has been localized. This is used to avoid
   * checking local file system for existence of the ticket.
   */
  private ConcurrentHashMap<ApplicationId, AtomicBoolean> accessKeyMap
    = new ConcurrentHashMap<ApplicationId, AtomicBoolean>();

  @Override
  public void run(ContainerId containerId, String username, Configuration conf) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    AtomicBoolean accessKey = getAccessKey(containerId.getApplicationAttemptId()
        .getApplicationId());

    // Avoid entering the critical section, if possible.
    // This is just an optimization.
    if (accessKey.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ticket already localized for " + containerId);
      }
      return;
    }

    synchronized (accessKey) {
      // This is the actual check.
      if (accessKey.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ticket already localized for " + containerId);
        }
        return;
      }

      try {
        localizeUserTicket(containerId, username, conf);
        accessKey.set(true);
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
    }
  }

  /**
   * Returns the access key for this application.
   */
  private AtomicBoolean getAccessKey(ApplicationId appId) {
    // Always initialize to false. true implies ticket as been localized.
    AtomicBoolean accessKey = new AtomicBoolean(false);
    AtomicBoolean existingAccessKey = accessKeyMap.putIfAbsent(appId, accessKey);

    // If there was already an access key in the map, it indicates that some
    // other container has already placed an access key. So this container
    // should reuse the same access key.
    if (existingAccessKey != null) {
      accessKey = existingAccessKey;
    }

    return accessKey;
  }

  @Override
  public Path getTokenPath(String appIdStr, Configuration conf) {
    return UserGroupInformation.isSecurityEnabled()
      ? YarnAppUtil.getNMPrivateTicketPath(appIdStr, conf)
      : null;
  }

  @Override
  public String getTokenEnvVar() {
    return "MAPR_TICKETFILE_LOCATION";
  }

  /**
   * Localize MapR ticket by downloading it from MapRFS and storing in the
   * private directory of NodeManager.
   *
   * Assumption: The caller ensures that no two containers belonging to the
   * same application will execute this code concurrently.
   */
  private void localizeUserTicket(ContainerId containerId, String username,
      Configuration conf)
    throws IOException {

    String appIdStr = containerId.getApplicationAttemptId()
      .getApplicationId().toString();

    Path localTicketPath = YarnAppUtil.getNMPrivateTicketPath(appIdStr, conf);

    // Get the ticket path on MapRFS
    FileSystem fs = FileSystem.get(conf);
    Path ticketPath = YarnAppUtil.getRMSystemMapRTicketPath(appIdStr, fs, conf);

    // Create the path on local disk for the ticket
    fs.copyToLocalFile(ticketPath, localTicketPath);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Localized user ticket file from "
          + ticketPath.toUri().getPath()
          + " to " + localTicketPath.toUri().getPath());
    }
  }

  @Override
  public void cleanup(ApplicationId appId) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    accessKeyMap.remove(appId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleanup complete for " + appId);
    }
  }
}
