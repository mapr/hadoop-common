/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

public class MapRIFileInputStream extends IFileInputStream {

	  private MapOutputFileInfo info = null;
	  boolean checkLength = false;
	  
	  /**
	   * Given input stream and length of file verify the filesize and provide a way to 
	   * extract mapoutputsize.
	   */
	  public MapRIFileInputStream(InputStream in, long len, Configuration conf) 
	  throws IOException {
	    this(in, len, true, conf);
	  }
	  
	  public MapRIFileInputStream(InputStream in, long len, boolean checkLength, Configuration conf) 
	  throws IOException {
	    // initialize IFileInputStream with length adjustment
	    super(in, (len - (in == null ? 0 : MapOutputFileInfo.MAPOUTPUT_INFO_BYTES)), conf);
	    if ( in != null ) {
	      this.checkLength = checkLength;
	      this.info = new MapOutputFileInfo((FSDataInputStream)in, len, checkLength);
	    }
	  }

	  public MapOutputFileInfo getMapOutputFileInfo() {
	    return info;
	  }

	  @Override public long getSize() {
	    if (!checkLength)
	      return 0L;
	    if ( this.info == null ) {
	      return super.getSize();
	    }
	    return MapOutputFileInfo.MAPOUTPUT_INFO_BYTES + super.getSize();
	  }
}
