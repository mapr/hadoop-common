package org.apache.hadoop.yarn;

import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

/**
 * WorldWritableLogAppender extends RollingFileAppender to create log file
 * with world-writable permissions
 */
public class WorldWritableLogAppender extends RollingFileAppender {

  @Override
  public synchronized void setFile(String fileName, boolean append,
                                   boolean bufferedIO, int bufferSize) throws IOException {
    File file = new File(fileName);
    File parent = file.getParentFile();
    boolean isFirstCreate = !file.exists();

    if (FileUtil.canWrite(file) || FileUtil.canWrite(parent)) {
      super.setFile(fileName, append, bufferedIO, bufferSize);
    }
    
    if (isFirstCreate && file.exists()) {
      Set<PosixFilePermission> permissions = new HashSet<>();
      permissions.add(PosixFilePermission.OWNER_READ);
      permissions.add(PosixFilePermission.OWNER_WRITE);
      permissions.add(PosixFilePermission.GROUP_READ);
      permissions.add(PosixFilePermission.GROUP_WRITE);
      permissions.add(PosixFilePermission.OTHERS_READ);
      permissions.add(PosixFilePermission.OTHERS_WRITE);

      Files.setPosixFilePermissions(file.toPath(), permissions);
    }
  }
}
