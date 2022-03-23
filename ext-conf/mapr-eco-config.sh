#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file is sourced by mapr-config.sh to provide get_hadoop_jars()
# get_hadoop_classpath() and get_hadoop_libpath()
# Assumes MAPR_HOME is set, and get_files_in_folder() is defined
# (This code was extracted from the Core mapr-config.sh in order to
#  separate non-core knowledge, and placing it with the Eco component)

export HADOOP_VERSION=`cat ${MAPR_HOME}/hadoop/hadoopversion`
export HADOOP_HOME="${MAPR_HOME}/hadoop/hadoop-${HADOOP_VERSION}"
export HADOOP_CONF=$HADOOP_HOME/etc/hadoop

# Get Hadoop jars
get_hadoop_jars() {
HADOOP_LIB_JARS=$(get_files_in_folder $HADOOP_HOME/share/hadoop/common/lib\
    "commons-cli-*.jar"\
    "commons-codec-*.jar"\
    "commons-io-*.jar"\
    "htrace-core*.jar")
echo $HADOOP_LIB_JARS
}

get_hadoop_classpath() {
  HADOOP_IN_PATH=$(PATH="${HADOOP_HOME:-${HADOOP_PREFIX}}/bin:$PATH"; which hadoop 2>/dev/null)
  if [ -f ${HADOOP_IN_PATH} ]; then
    ${HADOOP_IN_PATH} classpath 2>/dev/null
  fi
}

get_hadoop_libpath() {
  HADOOP_IN_PATH=$(PATH="${HADOOP_HOME:-${HADOOP_PREFIX}}/bin:$PATH"; which hadoop 2>/dev/null)
  if [ -f ${HADOOP_IN_PATH} ]; then
    ${HADOOP_IN_PATH} jnipath 2>/dev/null
  fi
}

