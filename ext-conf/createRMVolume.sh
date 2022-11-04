#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved

## Script to prepare jobtracker/resourcemanager/historyserver volume
## Called by JobTracker/ResourceManager/HistoryServer
# accepts hostname and mountpoint

MAPR_HOME=${MAPR_HOME:-__PREFIX_INSTALL__}

function usage() {
  echo >&2 "usage: $0 <hostname> <vol mount point> <<full directory path for ${SHORT_NAME} dirs>"
  exit 1
}

function logHeader() {
  #DATE| TIME| HOSTNAME | PROCESS(PID) | COMPONENT | SEVERITY | TRIGGER | MESSAGE
  cmdName=`basename "$0"`;
  now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
  logheader="$now $1 $cmdName($$) Install $2"
}

CheckIfTicketExist() {
 securityStatus=`head -n 1 $clusterConf | grep secure= | sed 's/^.*secure=//' | sed 's/[\t ].*$//'`
 if [ "$securityStatus" = "true" ]; then
   while [ ! -s $MAPRUSERTICKET ]; do
     echo "$MAPRUSERTICKET does not exist yet"  >> "$logFile" 2>&1
     sleep 3
   done
   export MAPR_TICKETFILE_LOCATION=${MAPR_HOME}/conf/mapruserticket
 fi
}

#################
#  main
# $HADOOP_HOME/bin/createRMVolume.sh node3.cluster.com /var/mapr/cluster/yarn/rm /var/mapr/cluster/yarn/rm yarn
# $HADOOP_HOME/bin/createRMVolume.sh node3.cluster.com /var/mapr/cluster/yarn/rm /var/mapr/cluster/yarn/rm yarn 0
#################

[ $# -ge 3 ] || {
  usage
}
hostname=$1
mountpath=`echo $2 | sed  's/\/*$//'`  # remove trailing slashes
fullpath=$3

# create parent of mountpath
parentpath=`dirname $mountpath`

# Set global variables
# Set variables specific to MR1 or YARN as this script is used by both JT and RM
if [ "$4" = "yarn" ] ; then
  SHORT_NAME="RM"
  LONG_NAME="ResourceManager"
  DIR_NAME="resourceManager"
  VOLUME_NAME="mapr.resourcemanager.volume"
elif [ "$4" = "hs" ] ; then
  SHORT_NAME="HS"
  LONG_NAME="HistoryServer"
  DIR_NAME="historyServer"
  VOLUME_NAME="mapr.historyserver.volume"
else
  # By default it is JT
  SHORT_NAME="JT"
  LONG_NAME="JobTracker"
  DIR_NAME="jobTracker"
  VOLUME_NAME="mapr.jobtracker.volume"
fi

volumeNumber=$5

if [ "$volumeNumber" != "" ] ; then
  VOLUME_NAME="$VOLUME_NAME"_"$volumeNumber"
  mountpath="$mountpath/$volumeNumber"
fi

clusterConf="${MAPR_HOME}/conf/mapr-clusters.conf"
MAPRUSERTICKET="${MAPR_HOME}/conf/mapruserticket"
logFile="${MAPR_HOME}/logs/create${SHORT_NAME}Volume.log"
pidFile="${MAPR_HOME}/pid/create${SHORT_NAME}Volume.sh.pid"

OLDMASK=`umask`
umask 0066
echo $$ > "$pidFile"
umask $OLDMASK

. ${MAPR_HOME}/server/scripts-common.sh
CheckIfTicketExist

# wait till parent path is available
i=0
while [ $i -lt 600 ]
do
  hadoop fs -stat $parentpath >> "$logFile" 2>&1
  if [ $? -eq 0 ] ; then
     i=9999
     break
  fi

  sleep 3
  i=$[i+3]
done

if [ $i -ne 9999 ] ; then
  echo --- $(date) --- Failed to detect parent dir $parentpath  >> "$logFile" 2>&1
  exit 1
fi
echo ---- $(date) --- ALL OK  >> "$logFile" 2>&1

# sanity check
if [ "$hostname" == "" ]; then
  echo >&2 "Error: Empty hostname"
  usage
fi

if [ "$mountpath" == "" ]; then
  echo >&2 "Error: Empty Mount Point"
  usage
fi

if [ "$fullpath" == "" ]; then
  echo >&2 "Error: Empty full path"
  usage
fi

## Lock volume creation so it is not done by multiple processes at the same time
## timeout for lock is 100 min
CheckForSingleInstanceWithTimeout 6000
# check if volume is present
maprcli volume info -name ${VOLUME_NAME}  > /dev/null 2>&1
if [ $? -ne 0 ]; then
  # create volume. Min repl is set to 3. Ideal replication factor is sqrt of nodes or  no. of racks
  maprcli volume create -name ${VOLUME_NAME} -path ${mountpath} -replication 3 >> "$logFile" 2>&1
  if [ $? -ne 0 ]; then
     logErr "${LONG_NAME} volume ${VOLUME_NAME} may already exist"
     echo >&2 "Failed to create and mount the ${LONG_NAME} volume."
     echo >&2 "Command used: maprcli volume create -name ${VOLUME_NAME} -path ${mountpath} -replication 3 "
     exit 1
  fi
else
  #check if volume is mounted
  volinfo=`maprcli volume info -name ${VOLUME_NAME} -json | grep mount`
  mounteddir=`echo $volinfo | awk -F "," '{print $1}' | sed 's/"//g' | sed 's/,//g' |  awk -F ":" '{print $2}'`
  mounted=`echo $volinfo  | awk -F "," '{print $2}' |  sed 's/"//g' | sed 's/,//g' |  awk -F ":" '{print $2}'`

  # check if its already mounted
  if [ $mounted -eq 1 ]; then
    # if volume is mounted on same path as given skip it
    if [ $mounteddir == $mountpath ]; then
       logInfo "${LONG_NAME} volume ${VOLUME_NAME} is already mounted at $mounteddir"
    else
       logErr "${LONG_NAME} volume ${VOLUME_NAME} is already mounted at $mounteddir."
       echo >&2 "Error: ${LONG_NAME} volume ${VOLUME_NAME} is already mounted at $mounteddir. Please unmount."
       exit 1
    fi
  else
    # simply mount it
    mount_output=`maprcli volume mount -name ${VOLUME_NAME} -path ${mountpath}`
    if [ $? -ne 0 ]; then
      echo $mount_output >>  "$logFile" 2>&1
      # Determines if we should exit the script or continue. This is needed because we have retry code below.
      error_exit=1

      # Check to see if the mountpath has been created by someone else outside the volume.
      # If so, delete the dir and retry mounting.
      hadoop fs -ls $mountpath
      if [ $? -eq 0 ]; then
        file_exists_pattern="ERROR (17) - Failed to mount volume ${VOLUME_NAME}, File exists"
        # Assumption: If there is a mount error because of existing dir, then it will be the last line of the output string.
        echo $mount_output | grep "$file_exists_pattern"
        if [ $? -eq 0 ]; then
          # Move the existing dir to a new location and retry mounting the volume.
          # Remove any trailing forward slash from the dir name.
          backup_dir=`echo ${mountpath} | sed s/[/]*$//`.BACK
          hadoop fs -mv ${mountpath} ${backup_dir}
    maprcli volume mount -name ${VOLUME_NAME} -path ${mountpath} >>  "$logFile" 2>&1
          if [ $? -eq 0 ]; then
            error_exit=0
          fi
        fi
      fi

      if [ $error_exit -eq 1 ]; then
      logErr "Failed to mount ${LONG_NAME} volume ${VOLUME_NAME} at ${mountpath}"
      echo >&2 "Failed to mount the ${LONG_NAME} volume."
      echo >&2 "Command used: maprcli volume mount -name ${VOLUME_NAME} -path ${mountpath}"
      exit 1
    fi
  fi
fi
fi

UnlockSingleInstance

# sleep for 3 seconds before creating directories
sleep 3
# create dirs, ignore errors
hadoop fs -mkdir -p $fullpath > /dev/null 2>&1
CURR_USER=`id -nu`
logInfo "hadoop fs -chown $CURR_USER $fullpath"
hadoop fs -chown $CURR_USER $fullpath > /dev/null 2>&1
hadoop fs -chown $CURR_USER $fullpath/../recovery > /dev/null 2>&1
exit 0