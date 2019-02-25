/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Handler to upload MapR ticket to Distribtued Cache so that it is available
 * for running the job.
 */
public abstract class AbstractMapRTicketUploader {

  private static final Log LOG = LogFactory.getLog(AbstractMapRTicketUploader.class);

  public abstract void uploadToken(ApplicationId appId, Configuration conf);

}
