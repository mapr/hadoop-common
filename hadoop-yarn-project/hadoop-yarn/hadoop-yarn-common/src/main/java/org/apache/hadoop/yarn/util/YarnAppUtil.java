/**
 * Copyright (c) 2014 & onwards. MapR Tech, Inc., All rights reserved
 */
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.conf.YarnDefaultProperties;

public class YarnAppUtil {
  /**
   * File permission to be used for any application specific directory.
   * It is rwx------. This ensures that only the app owner can access it.
   * mapr user is a special user and can therefore still access it.
   */
  public static final FsPermission APP_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700);

  /**
   * File permission used for any application specific file. It is world-wide
   * readable and owner writable : rw-r--r--. This is needed to make sure the
   * mapr user can read such files. Other users cannot read the file because
   * APP_DIR_PERMISSION disallows any user other than the app owner to access
   * it. mapr user is an exception to this rule.
   */
  final public static FsPermission APP_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0644);

  /**
   * Permission for the system directory on RM. This directory is used only by
   * mapr user. Hence no permission for other and group.
   */
  final public static FsPermission RM_SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700);

  /**
   * Permission for the staging directory on RM. This directory is used
   * to upload client specific files such as MapR ticket as the client
   * user itself and so needs to be accessible by all users.
   */
  final public static FsPermission RM_STAGING_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0777);

  private static final String MAPR_TICKET_FILE = "ticketfile";

  /**
   * Returns staging dir for the given app on resource manager.
   */
  public static Path getRMStagingDir(String appIdStr,
      FileSystem fs, Configuration conf) {

    Path dir = new Path(conf.get(YarnDefaultProperties.RM_STAGING_DIR));
    return new Path(fs.makeQualified(dir).toString(), appIdStr);
  }

  /**
   * Returns system dir for the given app on resource manager.
   */
  public static Path getRMSystemDir(String appIdStr,
      FileSystem fs, Configuration conf) {

    Path dir = new Path(conf.get(YarnDefaultProperties.RM_SYSTEM_DIR));
    return new Path(fs.makeQualified(dir).toString(), appIdStr);
  }

  /**
   * Returns the path of MapR ticket for the given app from the staging
   * directory on resource manager.
   */
  public static Path getRMStagedMapRTicketPath(String appIdStr,
      FileSystem fs, Configuration conf) {

    return getMapRTicketPath(getRMStagingDir(appIdStr, fs, conf));
  }

  /**
   * Returns the path of MapR ticket for the given app from the system
   * directory on resource manager.
   */
  public static Path getRMSystemMapRTicketPath(String appIdStr,
      FileSystem fs, Configuration conf) {

    return getMapRTicketPath(getRMSystemDir(appIdStr, fs, conf));
  }

  public static Path getMapRTicketPath(Path appDir) {
    return new Path(appDir, MAPR_TICKET_FILE);
  }

  /**
   * Returns the MapR ticket location relative to NodeManager private directory.
   */
  public static String getNMPrivateRelativeTicketLocation(String appIdStr) {
    StringBuilder sb = new StringBuilder();
    sb.append("nmPrivate")
      .append(Path.SEPARATOR)
      .append(appIdStr).append(Path.SEPARATOR)
      .append(MAPR_TICKET_FILE);

    return sb.toString();
  }

  /**
   * Returns the absolute MapR ticket path on NodeManager private directory.
   */
  public static Path getNMPrivateTicketPath(String appIdStr,
      Configuration conf) {
    return new Path(conf.get(YarnConfiguration.NM_LOCAL_DIRS),
        getNMPrivateRelativeTicketLocation(appIdStr));
  }
}
