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

package org.apache.hadoop.maprfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathId;

import java.io.IOException;

/**
 * Interface which provides base API for the MapR distributed file system interaction
 * with the file and its identifier (FID) within that file system.
 */
public interface Fid {

    FSDataOutputStream createFid(String pfid, String file) throws IOException;

    FSDataOutputStream createFid(String pfid, String file, boolean overwrite) throws IOException;

    FSDataInputStream openFid(String fid, long[] ips, long chunkSize, long fileSize) throws IOException;

    FSDataInputStream openFid(String pfid, String file, long[] ips) throws IOException;

    FSDataInputStream openFid2(PathId pfid, String file, int readAheadBytesHint) throws IOException;

    boolean deleteFid(String pfid, String dir) throws IOException;

    String mkdirsFid(Path p) throws IOException;

    String mkdirsFid(String pfid, String dir) throws IOException;

    void setOwnerFid(String fid, String user, String group) throws IOException;
}
