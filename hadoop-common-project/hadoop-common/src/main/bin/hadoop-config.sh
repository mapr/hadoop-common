#!/usr/bin/env bash
#
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

####
# IMPORTANT
####

## The hadoop-config.sh tends to get executed by non-Hadoop scripts.
## Those parts expect this script to parse/manipulate $@. In order
## to maintain backward compatibility, this means a surprising
## lack of functions for bits that would be much better off in
## a function.
##
## In other words, yes, there is some bad things happen here and
## unless we break the rest of the ecosystem, we can't change it. :(


# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
#
# after doing more config, caller should also exec finalize
# function to finish last minute/default configs for
# settings that might be different between daemons & interactive

# you must be this high to ride the ride
if [[ -z "${BASH_VERSINFO[0]}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 2 ]]; then
  echo "bash v3.2+ is required. Sorry."
  exit 1
fi

# In order to get partially bootstrapped, we need to figure out where
# we are located. Chances are good that our caller has already done
# this work for us, but just in case...

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _hadoop_common_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_hadoop_common_this}")" >/dev/null && pwd -P)
fi

# get our functions defined for usage later
if [[ -n "${HADOOP_COMMON_HOME}" ]] &&
   [[ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-functions.sh" ]]; then
  # shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh
  . "${HADOOP_COMMON_HOME}/libexec/hadoop-functions.sh"
elif [[ -e "${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh" ]]; then
  # shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh
  . "${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh"
else
  echo "ERROR: Unable to exec ${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh." 1>&2
  exit 1
fi

hadoop_deprecate_envvar HADOOP_PREFIX HADOOP_HOME

# allow overrides of the above and pre-defines of the below
if [[ -n "${HADOOP_COMMON_HOME}" ]] &&
   [[ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-layout.sh" ]]; then
  # shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-layout.sh.example
  . "${HADOOP_COMMON_HOME}/libexec/hadoop-layout.sh"
elif [[ -e "${HADOOP_LIBEXEC_DIR}/hadoop-layout.sh" ]]; then
  # shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-layout.sh.example
  . "${HADOOP_LIBEXEC_DIR}/hadoop-layout.sh"
fi


#Enable JMX for MaprMonitoring # tag for older collectd - do not remove
function logJmxMsg()
{
    echo $* >> "${YARN_JMX_LOG_FILE:-${HADOOP_HOME}/logs/${HADOOP_SUBCMD}_jmx_options.log}"
}

function configure_jmx() {
  COMMAND=$1

  isSecure="false"
  if [ -f "${MAPR_HOME}/conf/mapr-clusters.conf" ]; then
    isSecure=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
  fi

  #Mapr JMX handling
  MAPR_JMX_YARN_NODEMANAGER_PORT=${MAPR_JMX_YARN_NODEMANAGER_PORT:-8027}
  MAPR_JMX_YARN_PROXYSERVER_PORT=${MAPR_JMX_YARN_PROXYSERVER_PORT:-8026}
  MAPR_JMX_YARN_RESOURCEMANAGER_PORT=${MAPR_JMX_YARN_RESOURCEMANAGER_PORT:-8025}
  MAPR_JMX_YARN_SHAREDCACHEMANAGER_PORT=${MAPR_JMX_YARN_SHAREDCACHEMANAGER_PORT:-8028}
  MAPR_JMX_YARN_TIMELINESERVER_PORT=${MAPR_JMX_YARN_TIMELINESERVER_PORT:-8029}
  MAPR_JMX_YARN_HISTORYSERVER_PORT=${MAPR_JMX_YARN_HISTORYSERVER_PORT:-8024}
  if [ -z "$MAPR_JMXLOCALBINDING" ]; then
      MAPR_JMXLOCALBINDING="false"
  fi

  if [ -z "$MAPR_JMXAUTH" ]; then
      MAPR_JMXAUTH="false"
  fi

  if [ -z "$MAPR_JMXSSL" ]; then
      MAPR_JMXSSL="false"
  fi

  if [ -z "$MAPR_AUTH_LOGIN_CONFIG_FILE" ]; then
      MAPR_AUTH_LOGIN_CONFIG_FILE="${MAPR_HOME:-/opt/mapr}/conf/mapr.login.conf"
  fi

  if [ -z "$MAPR_JMX_LOGIN_CONFIG" ]; then
      MAPR_JMX_LOGIN_CONFIG="JMX_AGENT_LOGIN"
  fi

  if [ -z "$MAPR_JMXDISABLE" ] && [ -z "$MAPR_JMXLOCALHOST" ] && [ -z "$MAPR_JMXREMOTEHOST" ]; then
      logJmxMsg "No MapR JMX options given - defaulting to local binding"
  fi

  if [ -z "$MAPR_JMXDISABLE" ] || [ "$MAPR_JMXDISABLE" = 'false' ]; then
      # default setting for localBinding
      MAPR_JMX_OPTS="-Dcom.sun.management.jmxremote"

      if [ "$MAPR_JMXLOCALHOST" = "true" ] && [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
          logJmxMsg "WARNING: Both MAPR_JMXLOCALHOST and MAPR_JMXREMOTEHOST options are enabled - defaulting to MAPR_JMXLOCALHOST config"
          MAPR_JMXREMOTEHOST=false
      fi

      if [ "$isSecure" = "true" ] && [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
          JMX_JAR=$(echo ${MAPR_HOME:-/opt/mapr}/lib/jmxagent*)
          if [ -n "$JMX_JAR" ] && [ -f ${JMX_JAR} ]; then
              MAPR_JMX_OPTS="-javaagent:$JMX_JAR -Dmapr.jmx.agent.login.config=$MAPR_JMX_LOGIN_CONFIG \
                  -Djava.security.auth.login.config=$MAPR_AUTH_LOGIN_CONFIG_FILE"
              MAPR_JMXAUTH="true"
          else
              echo "jmxagent jar file missed"
              exit 1
          fi
      fi

      if [ "$MAPR_JMXAUTH" = "true" ]; then
          if [ "$isSecure" = "true" ]; then
              if [ -f "$MAPR_AUTH_LOGIN_CONFIG_FILE" ] && \
                 [ -f "${MAPR_HOME:-/opt/mapr}/conf/jmxremote.access" ]; then

                  MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=true \
                      -Dcom.sun.management.jmxremote.access.file=${MAPR_HOME:-/opt/mapr}/conf/jmxremote.access "

                if [ "$MAPR_JMXLOCALHOST" = "true" ]; then
                   MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Djava.security.auth.login.config=$MAPR_AUTH_LOGIN_CONFIG_FILE \
                     -Dcom.sun.management.jmxremote.login.config=$MAPR_JMX_LOGIN_CONFIG"
                fi
              else
                echo "JMX login config or access file missing - not starting since we are in secure mode"
                exit 1
              fi
          else
              echo "JMX Authentication configured - not starting since we are not in secure mode"
              exit 1
          fi
      else
          MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
      fi

      if [ "$MAPR_JMXLOCALHOST" = "true" ] || [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
          if [ "$MAPR_JMXSSL" = "true" ] && [ "$MAPR_JMXREMOTEHOST" = "true" ]; then
              MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=true"
          else
              MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"
          fi

          if [ "$MAPR_JMXLOCALHOST" = "true" ]; then
              MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Djava.rmi.server.hostname=localhost \
                -Dcom.sun.management.jmxremote.host=localhost \
                -Dcom.sun.management.jmxremote.local.only=true"
          fi

          case "$COMMAND" in
              resourcemanager)
                  if [ -z "$MAPR_JMX_YARN_RESOURCEMANAGER_ENABLE" ] || [ "$MAPR_JMX_YARN_RESOURCEMANAGER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_RESOURCEMANAGER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
              nodemanager)
                  if [ -z "$MAPR_JMX_YARN_NODEMANAGER_ENABLE" ] || [ "$MAPR_JMX_YARN_NODEMANAGER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_NODEMANAGER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
              historyserver)
                  if [ -z "$MAPR_JMX_YARN_HISTORYSERVER_ENABLE" ] || [ "$MAPR_JMX_YARN_HISTORYSERVER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_HISTORYSERVER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
              timelineserver|timelinereader)
                  # only enable is explicitly specified
                  if [ -n "$MAPR_JMX_YARN_TIMELINESERVER_ENABLE" ] && [ "$MAPR_JMX_YARN_TIMELINESERVER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_TIMELINESERVER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
              sharedcachemanager)
                  # only enable is explicitly specified
                  if [ -n "$MAPR_JMX_YARN_SHAREDCACHEMANAGER_ENABLE" ] && [ "$MAPR_JMX_YARN_SHAREDCACHEMANAGER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_SHAREDCACHEMANAGER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
              proxyserver)
                  if [ -n "$MAPR_JMX_YARN_PROXYSERVER_ENABLE" ] && [ "$MAPR_JMX_YARN_PROXYSERVER_ENABLE" = "true" ]; then
                      MAPR_JMX_PORT=${MAPR_JMX_YARN_PROXYSERVER_PORT}
                  else
                      MAPR_JMX_PORT=""
                      MAPR_JMX_OPTS=""
                  fi;;
          esac
          if [ -z "$MAPR_JMX_OPTS" ]; then
              logJmxMsg "WARNING: JMX disabled for $COMMAND"
          elif [ -z "$MAPR_JMX_PORT" ]; then
              logJmxMsg "WARNING: No JMX port given for $COMMAND - disabling TCP base JMX service"
              MAPR_JMX_OPTS=""
          else
              if [ "$MAPR_JMXREMOTEHOST" = "true" ] && [ "$isSecure" = "true" ]; then
                  MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dmapr.jmx.agent.port=$MAPR_JMX_PORT"
                  logJmxMsg "Enabling TCP JMX for $COMMAND on port $MAPR_JMX_PORT"
              else
                  if [ "$MAPR_JMXLOCALHOST" = "true" ]; then
                      logJmxMsg "Enabling TCP JMX for $COMMAND only on localhost port $MAPR_JMX_PORT"
                  else
                      logJmxMsg "Enabling TCP JMX for $COMMAND on port $MAPR_JMX_PORT"
                  fi
                  MAPR_JMX_OPTS="$MAPR_JMX_OPTS -Dcom.sun.management.jmxremote.port=$MAPR_JMX_PORT"
              fi
          fi
      fi

      if [ "$MAPR_JMXLOCALBINDING" = "true" ] && [ -z "$MAPR_JMX_OPTS" ]; then
          logJmxMsg "Enabling JMX local binding only"
          MAPR_JMX_OPTS="-Dcom.sun.management.jmxremote"
      fi
  else
      logJmxMsg "JMX disabled by user request"
      MAPR_JMX_OPTS=""
  fi

  hadoop_add_param HADOOP_OPTS service.jmx_config "$MAPR_JMX_OPTS"
  export MAPR_JMX_OPTS="${MAPR_JMX_OPTS}"
}

#
# IMPORTANT! We are not executing user provided code yet!
#

# Let's go!  Base definitions so we can move forward
hadoop_bootstrap

# let's find our conf.
#
# first, check and process params passed to us
# we process this in-line so that we can directly modify $@
# if something downstream is processing that directly,
# we need to make sure our params have been ripped out
# note that we do many of them here for various utilities.
# this provides consistency and forces a more consistent
# user experience


# save these off in case our caller needs them
# shellcheck disable=SC2034
HADOOP_USER_PARAMS=("$@")

hadoop_parse_args "$@"
shift "${HADOOP_PARSE_COUNTER}"

#
# Setup the base-line environment
#
hadoop_find_confdir
hadoop_exec_hadoopenv
hadoop_import_shellprofiles
hadoop_exec_userfuncs

#
# IMPORTANT! User provided code is now available!
#

hadoop_exec_user_hadoopenv
hadoop_verify_confdir

hadoop_deprecate_envvar HADOOP_SLAVES HADOOP_WORKERS
hadoop_deprecate_envvar HADOOP_SLAVE_NAMES HADOOP_WORKER_NAMES
hadoop_deprecate_envvar HADOOP_SLAVE_SLEEP HADOOP_WORKER_SLEEP

# do all the OS-specific startup bits here
# this allows us to get a decent JAVA_HOME,
# call crle for LD_LIBRARY_PATH, etc.
hadoop_os_tricks

hadoop_java_setup

hadoop_basic_init

# inject any sub-project overrides, defaults, etc.
if declare -F hadoop_subproject_init >/dev/null ; then
  hadoop_subproject_init
fi

hadoop_shellprofiles_init

# get the native libs in there pretty quick
hadoop_add_javalibpath "${HADOOP_HOME}/build/native"
hadoop_add_javalibpath "${HADOOP_HOME}/${HADOOP_COMMON_LIB_NATIVE_DIR}"

hadoop_shellprofiles_nativelib

# get the basic java class path for these subprojects
# in as quickly as possible since other stuff
# will definitely depend upon it.

hadoop_add_common_to_classpath
hadoop_shellprofiles_classpath

# user API commands can now be run since the runtime
# environment has been configured
hadoop_exec_hadooprc

#
# backwards compatibility. new stuff should
# call this when they are ready
#
if [[ -z "${HADOOP_NEW_CONFIG}" ]]; then
  hadoop_finalize
fi
