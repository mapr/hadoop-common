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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class MapOutputFileInfo {
  public static final int MAPOUTPUT_INFO_BYTES = 16;
  private long mapOutputSize = 0, fileBytesWritten = 0;
  private FSDataOutputStream out = null;

  public MapOutputFileInfo(FSDataOutputStream out, long rawLength, 
      long compressedLength) {
    this.out = out;
    this.mapOutputSize = rawLength;
    this.fileBytesWritten = compressedLength;
  }

  public MapOutputFileInfo(FSDataInputStream in, long fileSize)
  throws IOException {
    this (in, fileSize, true);
  }
  
  public MapOutputFileInfo(FSDataInputStream in, long fileSize, boolean checkLen)
  throws IOException {
    if (!checkLen) {
      this.mapOutputSize = 0L;
      this.fileBytesWritten = 0L;
    } else if (fileSize >= MAPOUTPUT_INFO_BYTES) {
      // save current position , required for spill file
      long pos = in.getPos();
      // seek to (end of file/segment - 16),  and read 2 long values
      in.seek(pos + (fileSize - MAPOUTPUT_INFO_BYTES));
      this.mapOutputSize = in.readLong(); // map output size
      this.fileBytesWritten = in.readLong(); // bytes written in file
      in.seek(pos);
      if ((fileBytesWritten + MAPOUTPUT_INFO_BYTES) !=  fileSize) {
        throw new IOException("Data Corruption in map output. " +
                        "[filesize, pos, mapOutputSize, fileBytesWritten] is " +
                        "[" + fileSize + ", " + pos + ", " + mapOutputSize +
                        ", " + fileBytesWritten + "]");
      }
    } else {
      throw new IOException("Data Corruption in map output. " +
                            "Incorrect fileSize " + fileSize);
    }
  }

  public long getMapOutputSize() {
    return this.mapOutputSize;
  }

  public long getFileBytesWritten() {
    return this.fileBytesWritten;
  }

  public void write() throws IOException {
    if (out != null) {
      // Write decompressed length and raw length.
      // Don't use variable length compression provided in WritableUtils
      // final file size = writer.getCompressedLength()+ 2*8
      out.writeLong(mapOutputSize); // size of map output
      out.writeLong(fileBytesWritten); //bytes written in file
    } else {
      throw new IOException("Failed to write. Output stream not initialized");
    }
  }
}
