/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.authorize;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@InterfaceStability.Stable
public class TestUsersACLsManager {

  /**
   * Test users ACL mapping.
   */
  @Test
  public void testUsersACLMapping() {
    Configuration conf = new Configuration();
    UsersACLsManager acl;

    //users mapping doesn't set
    acl = new UsersACLsManager(conf);
    assertFalse(acl.isUsersACLEnable());

    //set users mapping
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho=u:user1");
    acl = new UsersACLsManager(conf);
    assertTrue(acl.isUsersACLEnable());

    //check users access mapping
    assertTrue(acl.checkUserAccess("drwho", "user1"));

    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho=u:user1,u:user2");
    acl = new UsersACLsManager(conf);
    assertTrue(acl.checkUserAccess("drwho", "user1"));
    assertTrue(acl.checkUserAccess("drwho", "user2"));

    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho1=u:user1;u:drwho2=u:user2");
    acl = new UsersACLsManager(conf);
    assertTrue(acl.checkUserAccess("drwho1", "user1"));
    assertTrue(acl.checkUserAccess("drwho2", "user2"));

    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho1=u:user1,u:user2;u:drwho2=u:user1,u:user2");
    acl = new UsersACLsManager(conf);
    assertTrue(acl.checkUserAccess("drwho1", "user1"));
    assertTrue(acl.checkUserAccess("drwho1", "user2"));
    assertTrue(acl.checkUserAccess("drwho2", "user1"));
    assertTrue(acl.checkUserAccess("drwho2", "user2"));

    //configuration with spaces between groups
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho1=u:user1,u:user2;\n   u:drwho2=u:user1,u:user2");
    acl = new UsersACLsManager(conf);
    assertTrue(acl.checkUserAccess("drwho1", "user1"));
    assertTrue(acl.checkUserAccess("drwho1", "user2"));
    assertTrue(acl.checkUserAccess("drwho2", "user1"));
    assertTrue(acl.checkUserAccess("drwho2", "user2"));

    //user doesn't exists
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, "u:drwho1=u:user1,u:user2;u:drwho2=u:user1,u:user2");
    acl = new UsersACLsManager(conf);
    assertFalse(acl.checkUserAccess("drwho3", "user1"));
    assertFalse(acl.checkUserAccess("drwho1", "user3"));

    //config with group
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL,
        "u:drwho1,g:drgroup=g:group2,u:user1,u:user2;u:drwho2=u:user1,u:user2,g:group2");
    assertTrue(acl.checkUserAccess("drwho1", "user1"));

    //test exceptions
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL,
        " : =u:test");
    HadoopIllegalArgumentException ex = assertThrows(HadoopIllegalArgumentException.class, () ->
        new UsersACLsManager(conf));
    assertEquals("User/group entity is invalid: :", ex.getMessage());

    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL,
        "ut:testUser=u:test");
    ex = assertThrows(HadoopIllegalArgumentException.class, () ->
        new UsersACLsManager(conf));
    assertEquals("User/group entity is invalid: ut", ex.getMessage());

    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL,
        "u:drwho1,g:drgroup=test=test2 ");
    ex = assertThrows(HadoopIllegalArgumentException.class, () ->
        new UsersACLsManager(conf));
    assertEquals("Configuration yarn.users.acl.mapping is invalid", ex.getMessage());

    //empty config
    conf.set(CommonConfigurationKeys.HADOOP_USERS_ACL, " ");
    acl = new UsersACLsManager(conf);
    assertFalse(acl.isUsersACLEnable());
    assertFalse(acl.checkUserAccess("drwho3", "user1"));
    assertFalse(acl.checkUserAccess("drwho1", "user3"));
  }

}
