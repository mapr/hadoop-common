package org.apache.hadoop.security;

import java.util.ArrayList;
import java.util.List;

public class UserGroupMapping {
  public enum EntityType {

    USER("u"), GROUP("g");

    EntityType(String name) {
      this.name = name;
    }

    private final String name;

    @Override
    public String toString() {
      return this.name;
    }
  }

  private final EntityType type;

  private String name;

  private List<String> userList;
  private List<String> groupList;

  public UserGroupMapping(EntityType type) {
    this.type = type;
    this.userList = new ArrayList<>();
    this.groupList = new ArrayList<>();
    this.name = "";
  }

  public UserGroupMapping(EntityType type, String name) {
    this.type = type;
    this.name = name;
    this.userList = new ArrayList<>();
    this.groupList = new ArrayList<>();
  }

  public UserGroupMapping(EntityType type, String name, List<String> userList, List<String> groupList) {
    this.type = type;
    this.name = name;
    this.userList = userList;
    this.groupList = groupList;
  }

  public void addToUserList(String user) {
    userList.add(user);
  }

  public void addToGroupList(String group) {
    groupList.add(group);
  }

  public void addListToUserList(List<String> users) {
    userList.addAll(users);
  }

  public void addListToGroupList(List<String> groups) {
    groupList.addAll(groups);
  }

  public EntityType getType() {
    return type;
  }

  public void setName() {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setUserList(List<String> userList) {
    this.userList = userList;
  }

  public List<String> getUserList() {
    return userList;
  }

  public void setGroupList(List<String> groupList) {
    this.groupList = groupList;
  }

  public List<String> getGroupList() {
    return groupList;
  }

}
