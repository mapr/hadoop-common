package org.apache.hadoop.security.authorize;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.UserGroupInformation;

public class CaseInsensitiveAccessControlList extends AccessControlList {
  static {
    WritableFactories.setFactory
            (CaseInsensitiveAccessControlList.class,
                    new WritableFactory() {
                      @Override
                      public Writable newInstance() { return new CaseInsensitiveAccessControlList(); }
                    });
  }

  public CaseInsensitiveAccessControlList() {
    super();
  }

  public CaseInsensitiveAccessControlList(String aclString) {
    super(aclString.toLowerCase());
  }

  public CaseInsensitiveAccessControlList(String users, String groups) {
    super(users.toLowerCase(), groups.toLowerCase());
  }

  @Override
  public boolean isUserInList(UserGroupInformation ugi) {
    if (allAllowed || users.contains(ugi.getShortUserName().toLowerCase())) {
      return true;
    } else if (!groups.isEmpty()) {
      for (String group : ugi.getGroups()) {
        if (groups.contains(group.toLowerCase())) {
          return true;
        }
      }
    }
    return false;
  }
}
