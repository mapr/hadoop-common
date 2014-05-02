package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;

public class MapRIFileOutputStream extends IFileOutputStream {
  /**
   * Create a checksum output stream that writes
   * the bytes to the given stream.
   * @param out
   */
  public MapRIFileOutputStream(OutputStream out) {
    super(out);
  }

  /**
   * Finishes writing data to the output stream, by writing
   * the checksum bytes to the end. The underlying stream is not closed or flushed.
   * @throws IOException
   */
  @Override
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    finished = true;
    sum.writeValue(barray, 0, false);
    out.write (barray, 0, sum.getChecksumSize());
  }

  @Override
  public void finish(long decompBytes, long compBytes) throws IOException {
    if ( finished ) {
      return;
    }
    this.finish();
    MapOutputFileInfo info = new MapOutputFileInfo((FSDataOutputStream)out, decompBytes,
        compBytes + sum.getChecksumSize());
    info.write();
    return;
  }

}
