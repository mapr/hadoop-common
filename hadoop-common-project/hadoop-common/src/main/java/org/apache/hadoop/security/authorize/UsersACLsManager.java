package org.apache.hadoop.security.authorize;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for handling case when some user should have access to
 * application information of other users.
 * This possibility can be configured with users mapping
 * cur_user1=user1,user2;cur_user2=user3,user2
 */
public class UsersACLsManager {

  private Map<String, List<String>> usersAclMapping =
      new HashMap<String, List<String>>();

  public UsersACLsManager(Configuration conf) {
    String userACLs = conf.get(CommonConfigurationKeys.HADOOP_USERS_ACL, "").trim();
    if (!userACLs.equals("")) {
      parseMapping(userACLs);
    }
  }

  /**
   * Parser for users configuration mapping value
   */
  private void parseMapping(String userACLs) {
    Collection<String> mappings = StringUtils.getStringCollection(
        userACLs, ";");

    for (String users : mappings) {
      Collection<String> userToUser = StringUtils.getStringCollection(users.trim(),
          "=");
      if (userToUser.size() < 1 || userToUser.size() > 2) {
        throw new HadoopIllegalArgumentException("Configuration yarn.users.acl.mapping is invalid");
      }
      String[] userToGroupsArray = userToUser.toArray(new String[userToUser
          .size()]);
      String user = userToGroupsArray[0].trim();
      List<String> maprUsers = Collections.emptyList();
      if (userToGroupsArray.length == 2) {
        maprUsers = (List<String>) StringUtils
            .getStringCollection(userToGroupsArray[1]);
      }
      usersAclMapping.put(user, maprUsers);
    }
  }

  /**
   * Check if users ACLs mapping manager can be used
   * @return if any users was added to usersACLsMapping
   */
  public boolean isUsersACLEnable() {
    return !usersAclMapping.isEmpty();
  }

  /**
   * @param user current user
   * @param owner application owner
   * @return if current user can have access to application owner files
   */
  public boolean checkUserAccess(String user, String owner) {
    if (usersAclMapping.get(user) != null && usersAclMapping.get(user).contains(owner)) {
      return true;
    }
    return false;
  }
}
