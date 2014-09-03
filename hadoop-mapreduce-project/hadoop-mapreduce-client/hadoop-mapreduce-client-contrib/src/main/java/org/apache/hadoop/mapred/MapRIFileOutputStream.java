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
