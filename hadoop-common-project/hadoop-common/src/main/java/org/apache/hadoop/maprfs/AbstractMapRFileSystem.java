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
     * @throws IOException if an ACE could not be read/modified
     */
    public abstract int copyAce(Path src, Path dest) throws IOException;

    /**
     * This method fully replaces ACEs of a file or directory recursively,
     * discarding all existing ones.
     * Accepts ACE parameter as String in short format.
     * [rf:<ace>],[wf:<ace>],[ef:<ace>],[rd:<ace>],[ac:<ace>],[dc:<ace>],[ld:<ace>]
     * @param path path to set ACEs
     * @param strAces string of file ACEs in short format
     * @param isSet fully replace or stay existing ACE
     * @param noinherit do not inherit aces instead use umask
     * @param preservemodebits to not overwrite mode bits
     * @param recursive whether to apply ACE recursively true/false
     * @throws IOException if an ACE could not be replaced
     */
    public abstract void setAces(Path path, String strAces, boolean isSet, int noinherit,
                        int preservemodebits, boolean recursive) throws IOException;

    /**
     * This method deletes all ACEs of a file or directory.
     * @param path path to delete ACEs
     * @throws IOException if an ACEs could not be modified
     */
    public abstract void deleteAces(Path path, boolean recursive) throws IOException;
}
