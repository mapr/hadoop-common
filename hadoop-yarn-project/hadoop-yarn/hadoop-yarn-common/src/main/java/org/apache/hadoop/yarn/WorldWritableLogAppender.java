package org.apache.hadoop.yarn;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.io.IOException;

/**
 * WorldWritableLogAppender extends RollingFileAppender to create log file
 * with world-writable permissions
 */
public class WorldWritableLogAppender extends RollingFileAppender {

  @Override
  public synchronized void setFile(String fileName, boolean append,
                                   boolean bufferedIO, int bufferSize) throws IOException {
    Path path  = new Path(fileName);
    File file = new File(fileName);
    File parent = file.getParentFile();

    if (FileUtil.canWrite(file) || FileUtil.canWrite(parent)) {

      super.setFile(fileName, append, bufferedIO, bufferSize);

      FileContext lfs = FileContext.getLocalFSFileContext();
      FsPermission perm = FsPermission.createImmutable((short) 0666);
      if (!lfs.getFileStatus(path).getPermission().equals(perm)) {
        lfs.setPermission(path, perm);
      }
    }
  }
}
