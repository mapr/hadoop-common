/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.MapRCommonSecurityUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.YarnAppUtil;

/**
 * Handler to upload MapR ticket to Distribtued Cache so that it is available
 * for running the job.
 */
public class MapRTicketUploader  {
  private static final Logger LOG = LoggerFactory.getLogger(MapRTicketUploader.class);

  public void uploadToken(ApplicationId appId, Configuration conf) {
    // Upload only if security is enabled and the current user is not a proxy user
    try {
      if (!UserGroupInformation.isSecurityEnabled()
          || UserGroupInformation.getCurrentUser().getRealUser() != null) {

        return;
      }

      upload(appId.toString(), conf);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * Retrieves the ticket from known location on the node on which the job was
   * launched and uploads it to Distributed Cache.
   */
  private void upload(String appIdStr, Configuration conf) throws IOException {
    // Get ticket
    String ticketPath = MapRCommonSecurityUtil.getInstance().getUserTicketAndKeyFileLocation();
    if (ticketPath == null) {
      // If this happens it is some internal issue, since if file is not there
      // it would not pass login. So don't throw exception let RM handle it.
      LOG.warn("Security is enabled, but userTicketFile is null. May cause failures later.");
      return;
    }

    File ticketFile = new File(ticketPath);
    if (!ticketFile.exists() || !ticketFile.isFile()) {
      LOG.warn("Security is enabled, but userTicketFile cannot be found. May cause failures later.");
      return;
    }

    FileSystem fs = FileSystem.get(conf);
    Path appStagingDir = YarnAppUtil.getRMStagingDir(appIdStr, fs, conf);
    Path outTicketFilePath = YarnAppUtil.getMapRTicketPath(appStagingDir);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Uploading MapR ticket for app: " + appIdStr
          + " at " + outTicketFilePath + " . Source ticket: " + ticketPath);
    }

    // Create app staging dir
    FileSystem.mkdirs(fs, appStagingDir, YarnAppUtil.APP_DIR_PERMISSION);

    // Upload ticket
    FSDataOutputStream outTicket = FileSystem.create(fs, outTicketFilePath,
        new FsPermission(YarnAppUtil.APP_FILE_PERMISSION));

    FileInputStream fis = new FileInputStream(ticketFile);

    byte [] inBytes = new byte[1024];
    int ticketLen;
    try {
      while ((ticketLen = fis.read(inBytes)) != -1) {
        outTicket.write(inBytes, 0, ticketLen);
      }
    } finally {
      fis.close();
      outTicket.close();
    }
  }
}