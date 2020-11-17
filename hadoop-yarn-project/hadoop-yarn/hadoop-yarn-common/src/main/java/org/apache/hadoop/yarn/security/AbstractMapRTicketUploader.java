/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.security;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Handler to upload MapR ticket to Distribtued Cache so that it is available
 * for running the job.
 */
public abstract class AbstractMapRTicketUploader {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMapRTicketUploader.class);

  public abstract void uploadToken(ApplicationId appId, Configuration conf);

}
