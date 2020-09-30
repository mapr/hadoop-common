# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# MapR settings
BASEMAPR=${MAPR_HOME:-/opt/mapr}
env=${BASEMAPR}/conf/env.sh
[ -f $env ] && . $env

# export JAVA_HOME=/home/y/libexec/jdk1.6.0/

#MFS-6760: Fix warnings when using jdk 11
HADOOP_OPTS="$HADOOP_OPTS $MAPR_COMMON_JAVA_OPTS"
#MAPRHADOOP-107: Set ParallelGC by default on jdk11
HADOOP_OPTS="$HADOOP_OPTS -XX:+UseParallelGC"
#MAPRHADOOP-119: Skip "Logging initialized" messages
HADOOP_OPTS="$HADOOP_OPTS -Dorg.eclipse.jetty.util.log.announce=false"

export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=1000

export HADOOP_MAPRED_ROOT_LOGGER=INFO,RFA

export HADOOP_JOB_HISTORYSERVER_OPTS="${HADOOP_JOB_HISTORYSERVER_OPTS} ${MAPR_LOGIN_OPTS}"
#export HADOOP_MAPRED_LOG_DIR="" # Where log files are stored.  $HADOOP_MAPRED_HOME/logs by default.
#export HADOOP_JHS_LOGGER=INFO,RFA # Hadoop JobSummary logger.
#export HADOOP_MAPRED_PID_DIR= # The pid files are stored. /tmp by default.
#export HADOOP_MAPRED_IDENT_STRING= #A string representing this instance of hadoop. $USER by default
#export HADOOP_MAPRED_NICENESS= #The scheduling priority for daemons. Defaults to 0.
