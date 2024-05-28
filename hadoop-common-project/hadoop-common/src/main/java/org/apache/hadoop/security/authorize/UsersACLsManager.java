package org.apache.hadoop.security.authorize;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupMapping;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.UserGroupMapping.EntityType;

import java.util.ArrayList;
import java.util.Collection;
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

  private Map<String, UserGroupMapping> usersAclMapping =
      new HashMap<>();

  private Map<String, UserGroupMapping> groupAclMapping =
      new HashMap<>();

  private Map<String, UserGroupMapping> invertedUsersAclMapping =
      new HashMap<>();

  private Map<String, UserGroupMapping> invertedGroupAclMapping =
      new HashMap<>();

  public enum ACE {

    READ_FILE("rf"),
    WRITE_FILE("wf"),
    EXECUTE_FILE("ef"),
    READ_DIR("rd"),
    LOOKUP_DIR("ld"),
    ADD_CHILD("ac"),
    DELETE_CHILD("dc");

    ACE(String name) {
      this.name = name;
    }

    private final String name;

    @Override
    public String toString() {
      return this.name;
    }
  }

  public UsersACLsManager(Configuration conf) {
    String userACLs = conf.get(CommonConfigurationKeys.HADOOP_USERS_ACL, "").trim();
    if (!userACLs.isEmpty()) {
      parseMapping(userACLs);
    }
  }

  /**
   * Parse user/group ACL mapping
   *
   * @param userACLs string with user/group ACL lists
   */
  private void parseMapping(String userACLs) {
    Collection<String> mappingParts = StringUtils.getStringCollection(
        userACLs, ";");
    Map<EntityType, List<String>> reqLists;
    Map<EntityType, List<String>> mapLists;
    for (String part : mappingParts) {
      //u:user1,g:group1=u:user2,g:group2
      String[] mappingPart = StringUtils.getStrings(part.trim(),
          "=");
      if (mappingPart == null || mappingPart.length != 2) {
        throw new HadoopIllegalArgumentException("Configuration yarn.users.acl.mapping is invalid");
      }
      // mappingPart[0] - u:user1,g:group1
      // mappingPart[1] - u:user2,g:group2
      reqLists = parseUserGroupLists(mappingPart[0]);
      mapLists = parseUserGroupLists(mappingPart[1]);
      //add all users
      for (String userName : reqLists.get(EntityType.USER)) {
        UserGroupMapping userGroupValue = new UserGroupMapping(EntityType.USER, userName,
            mapLists.get(EntityType.USER), mapLists.get(EntityType.GROUP));

        usersAclMapping.put(userName, userGroupValue);
      }

      //add all groups
      for (String groupName : reqLists.get(EntityType.GROUP)) {
        UserGroupMapping userGroupValue = new UserGroupMapping(EntityType.GROUP, groupName,
            mapLists.get(EntityType.USER), mapLists.get(EntityType.GROUP));

        groupAclMapping.put(groupName, userGroupValue);
      }
    }
    invertAclMappring(usersAclMapping);
    invertAclMappring(groupAclMapping);
  }

  /**
   * Invert users ACL mapping. Inverted Map uses for log aggregation permissions
   * @param mapAclMapping ACL user mapping
   */
  private void invertAclMappring(Map<String, UserGroupMapping> mapAclMapping) {
    for (Map.Entry<String, UserGroupMapping> userSet : mapAclMapping.entrySet()) {
      List<String> ownerUserList = userSet.getValue().getUserList();
      for (String ownerUser : ownerUserList) {
        if (invertedUsersAclMapping.get(ownerUser) != null) {
          if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.USER)) {
            invertedUsersAclMapping.get(ownerUser).addToUserList(userSet.getKey());
          } else if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.GROUP)) {
            invertedUsersAclMapping.get(ownerUser).addToGroupList(userSet.getKey());
          }
        } else {
          UserGroupMapping userACLInfo = new UserGroupMapping(UserGroupMapping.EntityType.USER, ownerUser);
          if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.USER)) {
            userACLInfo.addToUserList(userSet.getKey());
          } else if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.GROUP)) {
            userACLInfo.addToGroupList(userSet.getKey());
          }
          invertedUsersAclMapping.put(ownerUser, userACLInfo);
        }
      }

      List<String> ownerGroupList = userSet.getValue().getGroupList();
      for (String ownerGroup : ownerGroupList) {
        if (invertedGroupAclMapping.get(ownerGroup) != null) {
          if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.USER)) {
            invertedGroupAclMapping.get(ownerGroup).addToUserList(userSet.getKey());
          } else if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.GROUP)) {
            invertedGroupAclMapping.get(ownerGroup).addToGroupList(userSet.getKey());
          }
        } else {
          UserGroupMapping userACLInfo = new UserGroupMapping(UserGroupMapping.EntityType.GROUP, ownerGroup);
          if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.USER)) {
            userACLInfo.addToUserList(userSet.getKey());
          } else if (userSet.getValue().getType().equals(UserGroupMapping.EntityType.GROUP)) {
            userACLInfo.addToGroupList(userSet.getKey());
          }
          invertedGroupAclMapping.put(ownerGroup, userACLInfo);
        }
      }
    }
  }

  private static Map<UserGroupMapping.EntityType, List<String>> parseUserGroupLists(String userGroupStr) {
    Map<EntityType, List<String>> parsedStringMap = new HashMap<>();
    List<String> userList = new ArrayList<>();
    List<String> groupList = new ArrayList<>();
    Collection<String> userGroupList = StringUtils.getStringCollection(userGroupStr.trim(), ",");
    for (String entity : userGroupList) {
      //u:user1
      String[] reqEntity = StringUtils.getStrings(entity.trim(), ":");
      if (reqEntity == null) {
        throw new HadoopIllegalArgumentException("User/group entity is invalid: " + entity.trim());
      }
      if (reqEntity.length != 2 ||
          !(reqEntity[0].equals(UserGroupMapping.EntityType.USER.toString()) ||
              reqEntity[0].equals(UserGroupMapping.EntityType.GROUP.toString()))) {
        throw new HadoopIllegalArgumentException("User/group entity is invalid: " + reqEntity[0]);
      }
      if (reqEntity[0].equals(UserGroupMapping.EntityType.USER.toString())) {
        userList.add(reqEntity[1]);
      }
      if (reqEntity[0].equals(UserGroupMapping.EntityType.GROUP.toString())) {
        groupList.add(reqEntity[1]);
      }
    }
    parsedStringMap.put(UserGroupMapping.EntityType.USER, userList);
    parsedStringMap.put(UserGroupMapping.EntityType.GROUP, groupList);
    return parsedStringMap;
  }

  /**
   * Check if users ACLs mapping manager can be used
   *
   * @return if any users was added to usersACLsMapping
   */
  public boolean isUsersACLEnable() {
    return !(usersAclMapping.isEmpty() && groupAclMapping.isEmpty());
  }

  /**
   * @param reqUser current user
   * @param owner   application owner
   * @return if current user can have access to application owner files
   */
  public boolean checkUserAccess(String reqUser, String owner) {
    return checkUserAccess(UserGroupInformation.createRemoteUser(reqUser), owner);
  }

  /**
   * @param reqUser current user
   * @param owner   application owner
   * @return if current user can have access to application owner files
   */
  public boolean checkUserAccess(UserGroupInformation reqUser, String owner) {
    // check users mapping
    if (!usersAclMapping.isEmpty() && usersAclMapping.get(reqUser.getShortUserName()) != null
        && !usersAclMapping.get(reqUser.getShortUserName()).getUserList().isEmpty()
        && usersAclMapping.get(reqUser.getShortUserName()).getUserList().contains(owner)) {
      return true;
    }

    List<String> ownerGroups = UserGroupInformation.createRemoteUser(owner).getGroups();
    if (!usersAclMapping.isEmpty() && usersAclMapping.get(reqUser.getShortUserName()) != null
        && !usersAclMapping.get(reqUser.getShortUserName()).getGroupList().isEmpty()) {
      //check request user to owner group mapping
      for (String expectedGroup : ownerGroups) {
        if (usersAclMapping.get(reqUser.getShortUserName()).getGroupList().contains(expectedGroup)) {
          return true;
        }
      }
    }

    if (!groupAclMapping.isEmpty()) {
      //check request group to owner user mapping
      List<String> reqGroups = reqUser.getGroups();
      for (String group : reqGroups) {
        if (groupAclMapping.get(group) != null && groupAclMapping.get(group).getUserList().contains(owner)) {
          return true;
        }
      }

      //check request group to owner group mapping
      for (String group : reqGroups) {
        if (groupAclMapping.get(group) != null) {
          for (String expectedGroup : ownerGroups) {
            if (groupAclMapping.get(group).getGroupList().contains(expectedGroup)) {
              return true;
            }
          }
        }
      }
    }

    return false;
  }

  /**
   * Build string with ACE to allow other users access to owner logs
   * @param owner - username of application owner
   * @return ACE in string format
   */
  public String buildACEStrForUser(String owner) {
    List<String> accessUser = invertedUsersAclMapping.get(owner).getUserList();
    List<String> accessGroup = invertedUsersAclMapping.get(owner).getGroupList();
    if (accessUser == null && accessGroup == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();

    //read owner and all allow users
    sb.append(ACE.READ_FILE);
    sb.append(":(");
    if (accessUser != null) {
      for (String user : accessUser) {
        sb.append("u:").append(user).append("|");
      }
    }
    if (accessGroup != null) {
      for (String user : accessGroup) {
        sb.append("g:").append(user).append("|");
      }
    }
    sb.append("u:").append(owner).append(")");

    //write only for owner
    sb.append(",").append(ACE.WRITE_FILE);
    sb.append(":(u:").append(owner).append(")");
    //execute only for owner
    sb.append(",").append(ACE.EXECUTE_FILE);
    sb.append(":(u:").append(owner).append(")");

    //read dir owner and all allow users
    sb.append(",").append(ACE.READ_DIR);
    sb.append(":(");
    if (accessUser != null) {
      for (String user : accessUser) {
        sb.append("u:").append(user).append("|");
      }
    }
    if (accessGroup != null) {
      for (String user : accessGroup) {
        sb.append("g:").append(user).append("|");
      }
    }
    sb.append("u:").append(owner).append(")");

    //lookup dir owner and all allow users
    sb.append(",").append(ACE.LOOKUP_DIR);
    sb.append(":(");
    if (accessUser != null) {
      for (String user : accessUser) {
        sb.append("u:").append(user).append("|");
      }
    }
    if (accessGroup != null) {
      for (String user : accessGroup) {
        sb.append("g:").append(user).append("|");
      }
    }
    sb.append("u:").append(owner).append(")");

    //add child only for owner
    sb.append(",").append(ACE.ADD_CHILD);
    sb.append(":(u:").append(owner).append(")");

    //delete child only for owner
    sb.append(",").append(ACE.DELETE_CHILD);
    sb.append(":(u:").append(owner).append(")");

    return sb.toString();
  }

}
