/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.conf;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_AUX_SERVICES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_CONTAINER_EXECUTOR;

import java.util.Properties;

//import org.apache.hadoop.conf.CoreDefaultProperties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpConfig;

import org.apache.hadoop.util.CommonMapRUtil;


/**
 * Default values for properties defined in yarn-site.xml.
 */
public class YarnDefaultProperties extends Properties {
  private static final long serialVersionUID = 42L;

  public static final String MAPR_TICKET_EXPIRY = YarnConfiguration.YARN_PREFIX
    + "mapr.ticket.expiration";

  // 7 days
  public static final String DEFAULT_MAPR_TICKET_EXPIRY = "604800000";

  public static final String CLUSTER_PREFIX = "cluster.name.prefix";

  public static final String RM_DIR = YarnConfiguration.RM_PREFIX + "dir";
  public static final String DEFAULT_RM_DIR = (System.getProperty(CLUSTER_PREFIX) != null) ? 
      "/var/mapr/cluster/yarn"+System.getProperty(CLUSTER_PREFIX)+"/rm"  : "/var/mapr/cluster/yarn/rm";

  public static final String RM_STAGING_DIR = YarnConfiguration.RM_PREFIX + "staging";
  public static final String DEFAULT_RM_STAGING_DIR = DEFAULT_RM_DIR + "/staging";

  public static final String RM_SYSTEM_DIR = YarnConfiguration.RM_PREFIX + "system";
  public static final String DEFAULT_RM_SYSTEM_DIR = DEFAULT_RM_DIR + "/system";

  public static final String RM_VOLUME_MANAGER_SERVICE = "RMVolumeManager";

  /**
   * Application history server volume manager service.
   */
  public static final String APP_HISTORY_VOLUME_MANAGER_SERVICE = "HSVolumeManager";

  public static final String APACHE_SHUFFLE_SERVICE_ID = "mapreduce_shuffle";
  public static final String MAPR_SHUFFLE_SERVICE_ID = "mapr_direct_shuffle";

  public static final String FAIR_SCHEDULER_CLASS =
    "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler";

  public static final String FS_DEFAULT_NAME = "maprfs:///";
  public static final String DEFAULT_MAPR_LOCAL_VOL_PATH = "/var/mapr/local";


  private static final boolean isSecurityEnabled;

  static {
    isSecurityEnabled = CommonMapRUtil.getInstance().isSecurityEnabled();
  }
  public YarnDefaultProperties() {
    // Dummy values needed to handle delegation token code path in TokenCache
    put(YarnConfiguration.RM_PRINCIPAL, "mapr");

    put(MAPR_TICKET_EXPIRY, DEFAULT_MAPR_TICKET_EXPIRY);

    if ( System.getProperty(CLUSTER_PREFIX) != null ) {
      put(CLUSTER_PREFIX, System.getProperty(CLUSTER_PREFIX));
    }
    put(RM_DIR, DEFAULT_RM_DIR);
    put(RM_STAGING_DIR, DEFAULT_RM_STAGING_DIR);
    put(RM_SYSTEM_DIR, DEFAULT_RM_SYSTEM_DIR);


    if (isSecurityEnabled) {
      put(YarnConfiguration.YARN_HTTP_POLICY_KEY,      // yarn-default.xml
          HttpConfig.Policy.HTTPS_ONLY.name());
    }

    put(YarnConfiguration.YARN_NODEMANAGER_EXT_TOKEN_LOCALIZER,
        "org.apache.hadoop.yarn.server.nodemanager.security.MapRTicketLocalizer");

    put(YarnConfiguration.YARN_EXT_TOKEN_MANAGER,
        "org.apache.hadoop.yarn.server.resourcemanager.security.MapRTicketManager");

    // RM auxiliary service
    put(YarnConfiguration.RM_AUX_SERVICES, RM_VOLUME_MANAGER_SERVICE);
    put(String.format(YarnConfiguration.AUX_SERVICE_FMT, RM_VOLUME_MANAGER_SERVICE),
        "org.apache.hadoop.yarn.server.resourcemanager.RMVolumeManager");

    // Application history auxiliary service
    put(YarnConfiguration.APPLICATION_HISTORY_AUX_SERVICES,
        APP_HISTORY_VOLUME_MANAGER_SERVICE);
    // The same volume is used by both RM and history server. Hence the same class is used.
    put(String.format(YarnConfiguration.AUX_SERVICE_FMT, APP_HISTORY_VOLUME_MANAGER_SERVICE),
        "org.apache.hadoop.yarn.server.resourcemanager.RMVolumeManager");

    // Configuration for RM's RPC services
    put(YarnConfiguration.RM_ADDRESS,
        "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_PORT);
    put(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    put(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    // Resource Management Configs.
    // The "$" variables will be set into the configuration set by Warden via environment.
    put(YarnConfiguration.NM_PMEM_MB, "${nodemanager.resource.memory-mb}");
    put(YarnConfiguration.NM_VCORES, "${nodemanager.resource.cpu-vcores}");
    put(YarnConfiguration.NM_DISKS, "${nodemanager.resource.io-spindles}");

    // Shuffle Aux Services Configuration
    put(NM_AUX_SERVICES, APACHE_SHUFFLE_SERVICE_ID + "," + MAPR_SHUFFLE_SERVICE_ID);
    put(NM_AUX_SERVICES + "." + APACHE_SHUFFLE_SERVICE_ID + ".class", "org.apache.hadoop.mapred.ShuffleHandler");
    put(NM_AUX_SERVICES + "." + MAPR_SHUFFLE_SERVICE_ID + ".class", "org.apache.hadoop.mapred.LocalVolumeAuxService");

    // container executor configuration
    put(NM_CONTAINER_EXECUTOR, "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor");

    // RM HA configs
    put(YarnConfiguration.RM_STORE, "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore");
    // state store dir will be created under this dir
    put(YarnConfiguration.FS_RM_STATE_STORE_URI, DEFAULT_RM_SYSTEM_DIR);
    put(YarnConfiguration.CUSTOM_RM_HA_RMFINDER, "org.apache.hadoop.yarn.client.MapRZKBasedRMAddressFinder");

    // Scheduler configs
    put(YarnConfiguration.RM_SCHEDULER, FAIR_SCHEDULER_CLASS);

    put(YarnConfiguration.LOG_AGGREGATION_ENABLED, "false");
    put(YarnConfiguration.ENABLE_DFS_LOGGING, "false");
    put(YarnConfiguration.DFS_LOGGING_HANDLER_CLASS, "org.apache.hadoop.yarn.server.utils.MapRFSLoggingHandler");

    put(YarnConfiguration.DFS_LOGGING_DIR_GLOB,
        FS_DEFAULT_NAME
        + DEFAULT_MAPR_LOCAL_VOL_PATH
        + Path.SEPARATOR
        + "*/logs/yarn/userlogs");

    // Default retention to 30 days
    put(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 30 * 24 * 3600 + "");
  }
}
