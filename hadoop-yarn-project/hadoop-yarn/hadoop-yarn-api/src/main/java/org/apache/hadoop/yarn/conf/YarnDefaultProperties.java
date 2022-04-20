/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.conf;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_AUX_SERVICES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_CONTAINER_EXECUTOR;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.MapRCommonSecurityUtil;

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

    public static final String YARN_DIR = "yarn.dir";
    public static final String DEFAULT_YARN_DIR = (System.getProperty(CLUSTER_PREFIX) != null) ?
            "/var/mapr/cluster/yarn"+System.getProperty(CLUSTER_PREFIX) : "/var/mapr/cluster/yarn";

    public static final String RM_DIR = YarnConfiguration.RM_PREFIX + "dir";
    public static final String DEFAULT_RM_DIR = DEFAULT_YARN_DIR + "/rm";

    public static final String RM_DIR_VOLUME_SHARDING_ENABLED = YarnConfiguration.RM_PREFIX + "dir.volume-sharding.enabled";
    public static final boolean DEFAULT_RM_DIR_VOLUME_SHARDING_ENABLED = false;

    public static final String RM_DIR_VOLUME_NEW_PATH_SUPPORT_ENABLED = YarnConfiguration.RM_PREFIX + "dir.new-volume-path-support.enabled";
    public static final boolean DEFAULT_RM_DIR_VOLUME_NEW_PATH_SUPPORT_ENABLED = false;

    public static final String RM_DIR_VOLUME_COUNT = YarnConfiguration.RM_PREFIX + "dir.volume-count";
    public static final int DEFAULT_RM_DIR_VOLUME_COUNT = 4;

    public static final String RM_STAGING_DIR = YarnConfiguration.RM_PREFIX + "staging";
    public static final String DEFAULT_RM_STAGING_DIR = DEFAULT_RM_DIR + "/staging";

    public static final String RM_SYSTEM_DIR = YarnConfiguration.RM_PREFIX + "system";
    public static final String DEFAULT_RM_SYSTEM_DIR = DEFAULT_RM_DIR + "/system";

    public static final String RM_VOLUME_MANAGER_SERVICE = "RMVolumeManager";

    /**
     * Application history server volume manager service.
     */
    public static final String APP_HISTORY_VOLUME_MANAGER_SERVICE = "HSVolumeManager";
    public static final String APP_HISTORY_STAGING_DIR = YarnConfiguration.APPLICATION_HISTORY_PREFIX + "staging";
    public static final String DEFAULT_APP_HISTORY_STAGING_DIR = (System.getProperty(CLUSTER_PREFIX) != null) ?
            "/var/mapr/cluster/hs"+System.getProperty(CLUSTER_PREFIX) : "/var/mapr/cluster/hs";

    public static final String APACHE_SHUFFLE_SERVICE_ID = "mapreduce_shuffle";
    public static final String MAPR_SHUFFLE_SERVICE_ID = "mapr_direct_shuffle";

    public static final String FAIR_SCHEDULER_CLASS =
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler";

    public static final String FS_DEFAULT_NAME = "maprfs:///";
    public static final String DEFAULT_MAPR_LOCAL_VOL_PATH = "/var/mapr/local";

    private static final boolean isSecurityEnabled;

    static {
        isSecurityEnabled = MapRCommonSecurityUtil.getInstance().isSecurityEnabled();
    }

    private static final Map<String, String> props =
            new HashMap<String, String>();

    static {
        // Dummy values needed to handle delegation token code path in TokenCache
        props.put(YarnConfiguration.RM_PRINCIPAL, "mapr");

        props.put(MAPR_TICKET_EXPIRY, DEFAULT_MAPR_TICKET_EXPIRY);

        if ( System.getProperty(CLUSTER_PREFIX) != null ) {
            props.put(CLUSTER_PREFIX, System.getProperty(CLUSTER_PREFIX));
        }
        props.put(RM_DIR, DEFAULT_RM_DIR);
        props.put(RM_DIR_VOLUME_COUNT, DEFAULT_RM_DIR_VOLUME_COUNT + "");
        props.put(RM_STAGING_DIR, DEFAULT_RM_STAGING_DIR);
        props.put(RM_SYSTEM_DIR, DEFAULT_RM_SYSTEM_DIR);
        props.put(APP_HISTORY_STAGING_DIR, DEFAULT_APP_HISTORY_STAGING_DIR);
        props.put(YARN_DIR, DEFAULT_YARN_DIR);

        if (isSecurityEnabled) {
            props.put(YarnConfiguration.YARN_HTTP_POLICY_KEY,      // yarn-default.xml
                    HttpConfig.Policy.HTTPS_ONLY.name());
        }

        props.put(YarnConfiguration.YARN_NODEMANAGER_EXT_TOKEN_LOCALIZER,
                "org.apache.hadoop.yarn.server.nodemanager.security.MapRTicketLocalizer");

        props.put(YarnConfiguration.YARN_EXT_TOKEN_MANAGER,
                "org.apache.hadoop.yarn.security.MapRTicketManager");

        // RM auxiliary service
        props.put(YarnConfiguration.RM_AUX_SERVICES, RM_VOLUME_MANAGER_SERVICE);
        props.put(String.format(YarnConfiguration.AUX_SERVICE_FMT, RM_VOLUME_MANAGER_SERVICE),
                "org.apache.hadoop.yarn.server.resourcemanager.RMVolumeManager");

        // Application history auxiliary service
        props.put(YarnConfiguration.APPLICATION_HISTORY_AUX_SERVICES,
                APP_HISTORY_VOLUME_MANAGER_SERVICE);
        props.put(String.format(YarnConfiguration.AUX_SERVICE_FMT, APP_HISTORY_VOLUME_MANAGER_SERVICE),
                "org.apache.hadoop.yarn.server.resourcemanager.RMVolumeManager");

        // Configuration for RM's RPC services
        props.put(YarnConfiguration.RM_ADDRESS,
                "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_PORT);
        props.put(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
        props.put(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
                "${" + YarnConfiguration.RM_HOSTNAME + "}:" + YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

        // Resource Management Configs.
        // The "$" variables will be set into the configuration set by Warden via environment.
        props.put(YarnConfiguration.NM_PMEM_MB, "${nodemanager.resource.memory-mb}");
        props.put(YarnConfiguration.NM_VCORES, "${nodemanager.resource.cpu-vcores}");
        //Added disk as resource
        //Convert disk value to long from double. Disk has value as milli resource type
        long disk = 2;
        if (System.getProperty("nodemanager.resource.io-spindles") != null) {
            disk = (long) (Double.parseDouble(System.getProperty("nodemanager.resource.io-spindles")) * 1000);
        }
        props.put(YarnConfiguration.RESOURCE_TYPES, "disks");
        props.put(YarnConfiguration.NM_RESOURCES_PREFIX + "disks", Long.toString(disk));


        // Shuffle Aux Services Configuration
        props.put(NM_AUX_SERVICES, APACHE_SHUFFLE_SERVICE_ID + "," + MAPR_SHUFFLE_SERVICE_ID);
        props.put(NM_AUX_SERVICES + "." + APACHE_SHUFFLE_SERVICE_ID + ".class", "org.apache.hadoop.mapred.ShuffleHandler");
        props.put(NM_AUX_SERVICES + "." + MAPR_SHUFFLE_SERVICE_ID + ".class", "org.apache.hadoop.mapred.LocalVolumeAuxService");

        // container executor configuration
        props.put(NM_CONTAINER_EXECUTOR, "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor");

        // RM HA configs
        props.put(YarnConfiguration.RM_STORE, "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore");
        // state store dir will be created under this dir
        props.put(YarnConfiguration.FS_RM_STATE_STORE_URI, DEFAULT_RM_SYSTEM_DIR);

        // TODO RM HA configs
//        put(YarnConfiguration.CUSTOM_RM_HA_RMFINDER, "org.apache.hadoop.yarn.client.MapRZKBasedRMAddressFinder");

        // Scheduler configs
        props.put(YarnConfiguration.RM_SCHEDULER, FAIR_SCHEDULER_CLASS);

        props.put(YarnConfiguration.LOG_AGGREGATION_ENABLED, "false");

        /* TODO DFS Logging */
//        put(YarnConfiguration.ENABLE_DFS_LOGGING, "false");
//        put(YarnConfiguration.DFS_LOGGING_HANDLER_CLASS, "org.apache.hadoop.yarn.util.MapRFSLoggingHandler");
//
//        put(YarnConfiguration.DFS_LOGGING_DIR_GLOB,
//                FS_DEFAULT_NAME
//                        + DEFAULT_MAPR_LOCAL_VOL_PATH
//                        + Path.SEPARATOR
//                        + "*/logs/yarn/userlogs");

        // Default retention to 30 days
        props.put(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 30 * 24 * 3600 + "");
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.putAll(props);
        return properties;
    }

    public YarnDefaultProperties() {
        this.putAll(props);
    }

}
