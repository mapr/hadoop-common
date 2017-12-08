/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.rpcauth.KerberosAuthMethod;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSecureIOUtils {

  private static String realOwner, realGroup;
  private static File testFilePathIs;
  private static File testFilePathRaf;
  private static File testFilePathFadis;
  private static FileSystem fs;

  @BeforeClass
  public static void makeTestFile() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRaw();
    testFilePathIs =
        new File((new Path("target", TestSecureIOUtils.class.getSimpleName()
            + "1")).toUri().getRawPath());
    testFilePathRaf =
        new File((new Path("target", TestSecureIOUtils.class.getSimpleName()
            + "2")).toUri().getRawPath());
    testFilePathFadis =
        new File((new Path("target", TestSecureIOUtils.class.getSimpleName()
            + "3")).toUri().getRawPath());
    for (File f : new File[] { testFilePathIs, testFilePathRaf,
        testFilePathFadis }) {
      FileOutputStream fos = new FileOutputStream(f);
      fos.write("hello".getBytes("UTF-8"));
      fos.close();
    }

    FileStatus stat = fs.getFileStatus(
        new Path(testFilePathIs.toString()));
    // RealOwner and RealGroup would be same for all three files.
    realOwner = stat.getOwner();
    realGroup = stat.getGroup();
  }

  @Test(timeout = 10000)
  public void testReadUnrestricted() throws IOException {
    SecureIOUtils.openForRead(testFilePathIs, null, null).close();
    SecureIOUtils.openFSDataInputStream(testFilePathFadis, null, null).close();
    SecureIOUtils.openForRandomRead(testFilePathRaf, "r", null, null).close();
  }

  @Test(timeout = 10000)
  public void testReadCorrectlyRestrictedWithSecurity() throws IOException {
    SecureIOUtils
        .openForRead(testFilePathIs, realOwner, realGroup).close();
    SecureIOUtils
        .openFSDataInputStream(testFilePathFadis, realOwner, realGroup).close();
    SecureIOUtils.openForRandomRead(testFilePathRaf, "r", realOwner, realGroup)
        .close();
  }

  @Test(timeout = 10000)
  public void testReadIncorrectlyRestrictedWithSecurity() throws IOException {
    // this will only run if libs are available
    assumeTrue(NativeIO.isAvailable());

    System.out.println("Running test with native libs...");
    String invalidUser = "InvalidUser";
    String invalidGroup = "InvalidGroup";

    // We need to make sure that forceSecure.. call works only if
    // the file belongs to expectedOwner.

    // InputStream
    try {
      SecureIOUtils
          .forceSecureOpenForRead(testFilePathIs, invalidUser, invalidGroup)
          .close();
      fail("Didn't throw exception for wrong user and group ownership!");

    } catch (IOException ioe) {
      // expected
    }

    // FSDataInputStream
    try {
      SecureIOUtils
          .forceSecureOpenFSDataInputStream(testFilePathFadis, invalidUser,
            invalidGroup).close();
      fail("Didn't throw exception for wrong user and group ownership!");
    } catch (IOException ioe) {
      // expected
    }

    // RandomAccessFile
    try {
      SecureIOUtils
          .forceSecureOpenForRandomRead(testFilePathRaf, "r", invalidUser,
            invalidGroup).close();
      fail("Didn't throw exception for wrong user and group ownership!");
    } catch (IOException ioe) {
      // expected
    }
  }

  @Test(timeout = 10000)
  public void testReadPermissions() throws IOException {
    // this will only run if libs are available
    assumeTrue(NativeIO.isAvailable());

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.CUSTOM_AUTH_METHOD_PRINCIPAL_CLASS_KEY,
      User.class.getName());
    conf.set(CommonConfigurationKeys.CUSTOM_RPC_AUTH_METHOD_CLASS_KEY,
      KerberosAuthMethod.class.getName());
    SecurityUtil.setAuthenticationMethod(
      UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    String invalidUser = "InvalidUser";
    String[] invalidGroups = {"invalidGroup1", "invalidGroup2", "invalidGroup3"};
    
    // Correct user
    SecureIOUtils
      .openForRead(testFilePathIs, realOwner, invalidGroups).close();
    // Correct group
    SecureIOUtils
      .openForRead(testFilePathIs, invalidUser, realGroup).close();
    // At least one common group
    String[] correctGroupInGroups = {"invalidGroup1", "invalidGroup2", "invalidGroup3", realGroup};
    SecureIOUtils
      .openForRead(testFilePathIs, invalidUser, correctGroupInGroups).close();

    // Invalid user and groups
    try {
      SecureIOUtils
        .openForRead(testFilePathIs, invalidUser, invalidGroups).close();
      fail("Didn't throw exception for wrong user and groups ownership!");
    } catch (PermissionNotMatchException e) {
      // expected
    }
  }

  @Test(timeout = 10000)
  public void testCreateForWrite() throws IOException {
    try {
      SecureIOUtils.createForWrite(testFilePathIs, 0777);
      fail("Was able to create file at " + testFilePathIs);
    } catch (SecureIOUtils.AlreadyExistsException aee) {
      // expected
    }
  }

  @AfterClass
  public static void removeTestFile() throws Exception {
    // cleaning files
    for (File f : new File[] { testFilePathIs, testFilePathRaf,
        testFilePathFadis }) {
      f.delete();
    }
  }
}
