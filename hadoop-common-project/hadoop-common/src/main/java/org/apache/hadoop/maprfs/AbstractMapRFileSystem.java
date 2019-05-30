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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathId;

import java.io.IOException;

/**
 * An abstract base class which represents top-level API for MapR-FS implementation of Hadoop {@link FileSystem}.
 * This class should be used from Hadoop side to get access to MapR-FS specific functionality.
 * The concrete MapR-FS implementation of the Hadoop distributed file system should extend this class
 * and provide an implementation of all declared methods.
 */
public abstract class AbstractMapRFileSystem extends FileSystem implements Fid {

    public abstract PathId createPathId();

    public abstract FSDataOutputStream createFid(String pfid, String file, boolean overwrite) throws IOException;

    /**
     * This method copies ACEs on source to destination.
     * @param src path of source
     * @param dest path of destination
     * @param recursive to apply recursively or not
     * @throws IOException if an ACE could not be read/modified
     */
    public abstract int copyAce(Path src, Path dest, boolean recursive) throws IOException;
}
