#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#
#  usage: createLocalVolumes.sh <hostname> <volume mountpoint> <full directory path for TT/NM dir> [yarn]
#

# Return codes
SUCCESS=0
INTERNAL_ERROR_C1=201
INTERNAL_ERROR_C2=202
COMMAND_FAILURE=203
CMDLOG_CREATION_FAILURE=204
CMDLOG_PERMISSION_FAILURE=205
LOG_CREATION_FAILURE=206
LOG_PERMISSION_FAILURE=207
VOLUME_REMOVAL_FAILED=208
MAPRFS_MOVE_FAILED=209
VOLUME_NOT_HEALTHY=210
UNKNOWN_VOLUME_MOUNTED=211
BAD_USAGE=212
VOLUME_LIST_FAILED=213
ERROR_ENTRY_NOT_FOUND=2
cmdRetCode=0

if [ "$4" = "yarn" ] ; then
  SHORT_NAME="NM"
  LONG_NAME="NodeManager"
  DIR_NAME="nodeManager"
elif [ "$4" = "staging" ] ; then
  SHORT_NAME="NM"
  LONG_NAME="Staging"
  DIR_NAME="staging"
elif [ "$4" = "spark" ]; then
  SHORT_NAME="NM"
  LONG_NAME="Spark"
  DIR_NAME="spark"
else
  # By default it is TT
  SHORT_NAME="TT"
  LONG_NAME="TaskTracker"
  DIR_NAME="taskTracker"
fi

function usage() {
    echo >&2 "usage: $0 <hostname> <volume mountpoint> <full directory path for $SHORT_NAME dir>"
    echo >&2 "Example:"
    echo >&2 "$0 `hostname -f` /var/mapr/local/`hostname -f`/mapred/ /var/mapr/local/`hostname -f`/mapred/$DIR_NAME/"
    exit $BAD_USAGE
}

#
# usage: runCommandWithTimeout <exit on error> <maxTimeSecs> <maxAttempts> <sleepBetweenSecs> <command>
#
# This function runs the specified command in the background, directing its output to a log file.
# If the command does not return with $commandTimeOut seconds, then a kill -9 will be issued against it.
# When a command attempt fails or is killed, it will be retried up to $maxAttempts times as long as $maxTime seconds has not elapsed since the first attempt
# If $maxTime elapses or the command has failed $maxAttempts times then the function will either exit or return $COMMAND_FAILURE
# If exitOnError is set to 1, a FATAL level message will be logged, the command output will be saved permanently and the script will exit
# If exitOnError is set to 0, a DEBUG level message will be logged and the function will return
function runCommandWithTimeout()
{
    # Parse the arguments to the funciton
    local exitOnError=$1
    local maxWaitSeconds=$2
    local maxAttempts=$3
    local sleepBetweenAttempts=$4
    local command=$5

    local checkForMaintenanceMode="true"
    if [ "$#" -gt "5" ] ; then
      checkForMaintenanceMode=$6
    fi

    if [ "n$sleepBetweenAttempts" = "n" ]; then
        sleepBetweenAttempts=0
    fi

    # Initialize some variables
    local oTime=`date +%s`
    local tElapsed=0
    local success=0
    local totalAttempts=0
    local commandTimeOut=60

    # Check that the function is called with acceptable arguments
    if [ $maxWaitSeconds -le 0 ]; then
        echo `date +"%Y-%m-%d %T"` FATAL max wait time was set \> 0 >> $logFile
        exit $INTERNAL_ERROR_C1
    elif [ $maxAttempts -le 0 ]; then
        echo `date +"%Y-%m-%d %T"` FATAL max attempts must be \> 0 >> $logFile
        exit $INTERNAL_ERROR_C2
    fi
    if [ $maxWaitSeconds -lt $commandTimeOut ]; then
        echo `date +"%Y-%m-%d %T"` DEBUG The max wait time of $maxWaitSeconds seconds is less than the individual command timeout of $commandTimeOut seconds, adjusting the individual command timeout to match the max wait seconds >> $logFile
        commandTimeOut=$maxWaitSeconds
    fi

    echo `date +"%Y-%m-%d %T"` DEBUG Will launch command \"$command\" with a command attempt timeout of $commandTimeOut seconds a maximum of $maxAttempts attempts and a sleep time of $sleepBetweenAttempts seconds between failed command attempts >> $logFile

    # Loop while the total elapsed time is less than the maximum amount of wait time and the command has not returned 0 in any previous attempt
    while [ $tElapsed -lt $maxWaitSeconds -a $totalAttempts -lt $maxAttempts -a $success -eq 0 ]
    do
        # Increment the attempt number/total
        totalAttempts=$(( $totalAttempts + 1 ))

        cmdRetCode=0
        # Launch the command, save the PID of child, record the time the command was launched and the time the command should be killed if it is still running
        echo `date +"%Y-%m-%d %T"` DEBUG Launching \"$command\" >> $logFile
        $command > $commandOutputFile 2>&1 &
        pid=$!
        sTime=`date +%s`
        eTime=$(( $sTime + $commandTimeOut ))

        # Sleep until the command has returned or until it has been running for the maximum allowed amount of time
        while [ -d /proc/$pid -a $eTime -gt `date +%s` ]; do
            sleep 1
        done

        # If the command is still running, kill it and note that the command did not exit on its own by setting wasKilled=1
        if [ -d /proc/$pid ]; then
            echo `date +"%Y-%m-%d %T"` DEBUG Command did not complete within $commandTimeOut seconds, issuing \"kill -9 $pid\" >> $logFile
            wasKilled=1
            kill -9 $pid
            ret=$?
            if [ $ret != 0 ]; then
                echo `date +"%Y-%m-%d %T"` WARN kill exited with error, return code was $ret >> $logFile
            else
                echo `date +"%Y-%m-%d %T"` DEBUG Successfully killed process >> $logFile
            fi
        else
            wasKilled=0
        fi

        # Retrieve the return code of the command
        wait $pid
        cmdRetCode=$?

        cElapsed=`date +%s`
        cElapsed=$(( $cElapsed - $sTime ))

        # Log the results of the command attempt
        if [ $wasKilled -eq 0 ]; then
            if [ $cmdRetCode -eq 0 ]; then
                echo `date +"%Y-%m-%d %T"` DEBUG Command attempt $totalAttempts completed successfully in $cElapsed seconds >> $logFile
                success=1
            else
                echo `date +"%Y-%m-%d %T"` DEBUG Command attempt $totalAttempts failed with return code $ret after $cElapsed seconds, sleeping for $sleepBetweenAttempts seconds >> $logFile
                sleep $sleepBetweenAttempts > /dev/null 2> /dev/null
            fi
        else
            echo `date +"%Y-%m-%d %T"` DEBUG Command attempt $totalAttempts failed to return within $commandTimeOut seconds, it was killed and returned code $cmdRetCode, exiting >> $logFile
        fi

        # Calculate how much time has elapsed since the first command attempt was launched
        tElapsed=`date +%s`
        tElapsed=$(( $tElapsed - $oTime ))
    done

    # Determine what should be done now that we have the results of the command attempts
    if [ $success -eq 1 ]; then
        # If the command completed succesfully then log success
        echo `date +"%Y-%m-%d %T"` DEBUG Command completed successfully after $totalAttempts attempts and after $tElapsed seconds >> $logFile
    else # If the command did not complete successfully during any attempt
        # Exit with $COMMAND_FAILURE if the script should exit if the command fails
        if [ $exitOnError -eq 1 ]; then
            if [ "$checkForMaintenanceMode" == "true" ] ; then
              checkFsInMaintenanceMode
            fi
            if [ $fsUnderMaintenance -eq 1 ]; then
                echo `date +"%Y-%m-%d %T"` FATAL Fileserver is under maintenance. Retry after bringing the fileserver out of maintenance mode. >> $logFile
            else
                echo `date +"%Y-%m-%d %T"` FATAL Command did not complete successfully after $totalAttempts attempts and after $tElapsed seconds. >> $logFile
                echo `date +"%Y-%m-%d %T"` INFO The command run was: >> $logFile
                echo "$command" >> $logFile
                echo >> $logFile
                echo `date +"%Y-%m-%d %T"` INFO The output of the last failed command attempt: >> $logFile
            fi
            cat $commandOutputFile >> $logFile
            exit $COMMAND_FAILURE
        else # Otherwise, log that the command failed
            echo `date +"%Y-%m-%d %T"` DEBUG Command did not complete successfully after $totalAttempts attempts and after $tElapsed seconds >> $logFile
            return $COMMAND_FAILURE
        fi
    fi
    return 0
}

function removeDirAtVolumePath() {
    echo `date +"%Y-%m-%d %T"` WARN A directory already exists at the same path where the $LONG_NAME volume should be mounted, the directory will be removed. >> $logFile
    runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $parentpath/old.mapred"
    if [ $? -ne 0 ]; then
        runCommandWithTimeout 1 360 3 1 "hadoop fs -mkdir $parentpath/old.mapred"
    fi
    runCommandWithTimeout 0 60 1 1 "hadoop fs -mv $mountpath $parentpath/old.mapred/`date +%s`"
    runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $mountpath"
    if [ $? -eq 0 ]; then
        echo `date +"%Y-%m-%d %T"` FATAL Failed to move the existing directory at the mountpath $mountpath to the old.mapred directory at $parentpath/old.mapred for deletion >> $logFile
        exit $MAPRFS_MOVE_FAILED
    fi
    nohup hadoop fs -rmr $parentpath/old.mapred > /dev/null 2> /dev/null &
    echo `date +"%Y-%m-%d %T"` INFO The pre-existing directory is being deleted in the background by process $!  >> $logFile
}

function createNewTTVolume() {
    echo `date +"%Y-%m-%d %T"` INFO A new $LONG_NAME volume will be created.  >> $logFile
    if [ $computeNode -eq 0]; then
        runCommandWithTimeout 1 180 3 1 "maprcli volume create -name $vol -path $mountpath -replication 1 -localvolumehost $hostname -localvolumeport $mfsport -shufflevolume true -rereplicationtimeoutsec 300"
    else
        runCommandWithTimeout 1 180 3 1 "maprcli volume create -name $vol -path $mountpath -replication 1 -shufflevolume true -rereplicationtimeoutsec 300"
    fi
    runCommandWithTimeout 1 180 3 1 "maprcli volume info -name $vol -json"
    mountdir=`grep \"mountdir\": $commandOutputFile | cut -f 4 -d \"`
    mounted=`grep \"mounted\": $commandOutputFile | cut -f 2 -d : | cut -f 1 -d ,`
    runCommandWithTimeout 1 180 3 1 "maprcli alarm list -type VOLUME -entity $vol"
    dataAlarmRaised=`grep -e DATA_UNDER_REPLICATED -e DATA_UNAVAILABLE $commandOutputFile | wc -l`
    if [ $mounted -eq 1 -a "n$mountdir" = "n$mountpath" -a $dataAlarmRaised -eq 0 ]; then
        echo `date +"%Y-%m-%d %T"` INFO Successfully created new volume and checked that it is healthy >> $logFile
        volumeOK=1
    else
        echo `date +"%Y-%m-%d %T"` FATAL Created a new volume but it is not healthy >> $logFile
        exit $VOLUME_NOT_HEALTHY
    fi
}

function checkPathAndCreateNewTTVolume() {
    runCommandWithTimeout 1 180 3 1 "hadoop mfs -ls $parentpath"
    pathPresent=`grep $mountpath$ $commandOutputFile | wc -l`
    isVolume=`grep $mountpath$ $commandOutputFile | grep ^v | wc -l`
    if [ $pathPresent -ne 0 -a $isVolume -eq 0 ]; then
        removeDirAtVolumePath
        createNewTTVolume
    elif [ $pathPresent -ne 0 -a $isVolume -ne 0 ]; then
        # Should not be reached, since we should have unmounted it when "Checking for a volume already mounted at the specified mount path"
        echo `date +"%Y-%m-%d %T"` FATAL A volume is already mounted at the mountpath for the $LONG_NAME volume but it is not the expected volume, hadoop mfs -ls output follows >> $logFile
        cat $commandOutputFile >> $logFile
        exit $UNKNOWN_VOLUME_MOUNTED
    elif [ $pathPresent -eq 0 ]; then
        createNewTTVolume
    fi
}

function checkFsInMaintenanceMode() {
    runCommandWithTimeout 1 180 3 1 "maprcli node list -filter [hostname==$hostname] -columns h -json" "false"
    nodeHealth=`grep -w health $commandOutputFile | awk -F "[:,]" '{print $2}'`
    if [ $nodeHealth -eq 3 ]; then
      fsUnderMaintenance=1
    fi
}

#################
#  main
#################

# Initialize some global variables
server=$(dirname $0) # is most likely /opt/mapr/server
INSTALL_DIR=$(cd "$server/../"; pwd)
MAPRUSERTICKET="${INSTALL_DIR}/conf/mapruserticket"
clusterConf="${INSTALL_DIR}/conf/mapr-clusters.conf"
logFile="${INSTALL_DIR}/logs/create${SHORT_NAME}Volume.`id -u`.log"
commandOutputFile="${INSTALL_DIR}/logs/create${SHORT_NAME}Volume.`id -u`.cmd.out"
. $INSTALL_DIR/server/scripts-common.sh
exitForUsage=0
fsUnderMaintenance=0
computeNode=0

# Parse arguments to script
hostname=$1
mountpath=`echo $2 | sed  's/\/*$//'`  # remove trailing slashes
fullpath=$3

# Check arguments to script
if [ $# -lt 3 ]; then
    echo >&2 "ERROR Did not detect the expected number of arguments"
    exitForUsage=1
fi
if [ "n$hostname" == "n" ]; then
    echo >&2 "ERROR Empty hostname"
    exitForUsage=1
fi
if [ "n$mountpath" == "n" ]; then
    echo >&2 "ERROR Empty Mount Point"
    exitForUsage=1
fi
if [ "n$fullpath" == "n" ]; then
    echo >&2 "ERROR Empty full path"
    exitForUsage=1
fi
if [ $exitForUsage -eq 1 ]; then
    usage
    exit $BAD_USAGE
fi

#Detect computeNode
if [ ! -f $INSTALL_DIR/roles/fileserver ]; then
    computeNode=1
    echo `date +"%Y-%m-%d %T"` INFO This is a compute node. Creating NM volumes on remote MFS >> $logFile
fi

if [ $computeNode -eq 0 ]; then
    #Detect the MFS port
    mfsconf=$INSTALL_DIR/conf/mfs.conf
    mfsport=$(awk -F= '/^mfs.server.port/ {print $2}' $mfsconf)
    if [ "$mfsport" == "" ]; then
        echo >&2 "INFO mfs port not present in $mfsconf, using default setting of 5660"
        mfsport=5660
    fi
else
    #Set default mfs port for compute node
    mfsport=5660
fi

# setup log files
touch $logFile
if [ $? -ne 0 ]; then
    echo Failed to create file: $logFile
    exit $LOG_CREATION_FAILURE
fi
chmod 744 $logFile
if [ $? -ne 0 ]; then
    echo Failed to set permissions on file: $logFile to 744
    exit $LOG_PERMISSION_FAILURE
fi
touch $commandOutputFile
if [ $? -ne 0 ]; then
    echo Failed to create file: $commandOutputFile
    exit $CMDLOG_CREATION_FAILURE
fi
chmod 700 $commandOutputFile
if [ $? -ne 0 ]; then
    echo Failed to set permissions on file: $commandOutputFile to 700
    exit $CMDLOG_PERMISSION_FAILURE
fi
echo > $commandOutputFile
echo `date +"%Y-%m-%d %T"` INFO This script was called with the arguments: $@ >> $logFile

# find security ticket
securityStatus=`head -n 1 $clusterConf | grep secure= | sed 's/^.*secure=//' | sed 's/[\t ].*$//'`
if [ "$securityStatus" = "true" ]; then
    echo `date +"%Y-%m-%d %T"` INFO Checking if ${MAPRUSERTICKET} exists >> $logFile
    while [ ! -s $MAPRUSERTICKET ]; do
        echo `date +"%Y-%m-%d %T"` WARN ${MAPRUSERTICKET} does not exist yet >> "$logFile" 2>&1
        sleep 3
    done
    export MAPR_TICKETFILE_LOCATION=${INSTALL_DIR}/conf/mapruserticket
fi

# create parent of mountpath
#
if [ "$4" = "staging" ] ; then
  vol="mapr.$hostname.local.nmstaging"
elif [ "$4" = "spark" ] ; then
  vol="mapr.$hostname.local.spark"
else
  vol="mapr.$hostname.local.mapred"
fi

parentpath=${mountpath%/*}  #  dirname of $mountpath
volumeOK=0
keepExistingContent=0

echo `date +"%Y-%m-%d %T"` INFO Checking if MapRFS is online >> $logFile
runCommandWithTimeout 1 600 1000 1 "hadoop fs -stat /"
if [ $computeNode -eq 0 ]; then
    runCommandWithTimeout 1 60 1000 1 "hadoop fs -stat $parentpath"
    echo `date +"%Y-%m-%d %T"` INFO MapRFS is online.  Checking whether MFS on this node is online >> $logFile
    runCommandWithTimeout 1 300 60 3 "$server/mrconfig -p $mfsport info fsstate"
    echo `date +"%Y-%m-%d %T"` INFO MFS on this node is online >> $logFile
else
    hadoop fs -test -d $parentpath
    if [ $? -ne 0 ]; then
        runCommandWithTimeout 1 60 1000 1 "hadoop fs -mkdir -p $parentpath"
    fi
fi
echo `date +"%Y-%m-%d %T"` INFO Checking for a volume already mounted at the specified mount path >> $logFile
runCommandWithTimeout 0 60 1 1 "maprcli volume list -filter [p==$mountpath]and[mt==1] -columns volumename,mounted,mountdir -json"
ret=$?
if [ $ret -eq 0 ]; then
    numResults=`grep -e "\"total\":[0-9]*" $commandOutputFile | cut -f 2 -d : | tr -dc '0-9'`
    if [ $numResults -eq 0 ]; then
      echo `date +"%Y-%m-%d %T"` INFO The mount path is not currently being used as the primary mount path of any existing volume >> $logFile
    elif [ $numResults -eq 1 ]; then
      mountDirCorrect=`grep -e "\"mountdir\":\"$mountpath\"" $commandOutputFile | wc -l`
      nameOfVolumeAtMountPath=`grep -e "\"volumename\":\".*\"" $commandOutputFile | cut -f 4 -d \"`
      mounted=`grep -e "\"mounted\":1" $commandOutputFile | wc -l`
      if [ $mounted -ne 1 ]; then
        echo `date +"%Y-%m-%d %T"` ERROR maprcli volume list returned unmounted volumes at $mountpath despite filter requesting only mounted volumes >> $logFile
        cat $commandOutputFile >> $logFile
        exit $VOLUME_LIST_FAILED
      elif [ $mountDirCorrect -ne 1 ]; then
        echo `date +"%Y-%m-%d %T"` ERROR maprcli volume list returned volumes mounted at locations other than $mountpath despite filter >> $logFile
        cat $commandOutputFile >> $logFile
        exit $VOLUME_LIST_FAILED
      elif [ "n$nameOfVolumeAtMountPath" != "n$vol" ]; then
        echo `date +"%Y-%m-%d %T"` WARN The volume \"$nameOfVolumeAtMountPath\" is already mounted at the mount point, unmounting it >> $logFile
        runCommandWithTimeout 1 60 3 1 "maprcli volume unmount -name $nameOfVolumeAtMountPath"
      else
        echo `date +"%Y-%m-%d %T"` INFO The expected volume \"$vol\" is already mounted at \"$mountpath\" >> $logFile
      fi
    else
      echo `date +"%Y-%m-%d %T"` ERROR maprcli volume list returned $numResults volumes mounted at $mountpath when there should be only one>> $logFile
      cat $commandOutputFile >> $logFile
      exit $VOLUME_LIST_FAILED
    fi
else
    echo `date +"%Y-%m-%d %T"` FATAL maprcli volume list exited with error code $ret, exiting >> $logFile
    cat $commandOutputFile >> $logFile
    exit $VOLUME_LIST_FAILED
fi

## Lock volume check and creation with timeout 100 min
CheckForSingleInstanceWithTimeout 6000

echo `date +"%Y-%m-%d %T"` INFO Checking for a pre-existing $LONG_NAME volume >> $logFile
runCommandWithTimeout 0 60 1 1 "maprcli volume info -name $vol -json"
if [ $? -eq 0 ]; then
    echo `date +"%Y-%m-%d %T"` INFO $LONG_NAME volume already exists, checking on the volume status >> $logFile
    repfactor=`grep \"numreplicas\":\"1\" $commandOutputFile | wc -l`
    rackpath=$(grep \"rackpath\":\" $commandOutputFile | grep /`hostname -f`\",$ $commandOutputFile | wc -l)
    notreadonly=`grep \"readonly\":\"0\", $commandOutputFile | wc -l`
    mountdir=`grep \"mountdir\": $commandOutputFile | cut -f 4 -d \"`
    mounted=`grep \"mounted\": $commandOutputFile | cut -f 2 -d : | cut -f 1 -d ,`
    volid=`grep \"volumeid\": $commandOutputFile | cut -f 2 -d : | cut -f 1 -d ,`
    rootcid=`grep \"nameContainerId\": $commandOutputFile | cut -f 2 -d : | cut -f 1 -d ,`
    repairedVolume=1;

    if [ $volid -ne 0 ]; then
      echo `date +"%Y-%m-%d %T"` INFO NodeManager volume already exists, checking if the volume can be re-used >> $logFile
      runCommandWithTimeout 0 180 1 1 "$server/mrconfig -p $mfsport volume checkrepaired $volid $rootcid"
      repairedVolume=$?

      if [ $cmdRetCode -ne 0 -a $cmdRetCode -ne $ERROR_ENTRY_NOT_FOUND ]; then
        # fileserver returns ENOENT(2) to indicate that the volume needs to be re-created
        echo `date +"%Y-%m-%d %T"` ERROR 'volume checkrepaired' failed with error $cmdRetCode, exiting script, caller should retry after some time >> $logFile
        exit $cmdRetCode
      fi
    fi

    if [ $repfactor -eq 1 -a $notreadonly -eq 1 -a $mounted -eq 1 -a "n$mountdir" = "n$mountpath" -a $repairedVolume -eq 0 ] && [[ $rackpath -eq 1 || $computeNode -eq 1 ]]; then
        echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume is healthy and mounted at the correct path >> $logFile
        volumeOK=1
        keepExistingContent=1
    else
        if [ $mounted -eq 0 ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume is not mounted >> $logFile
        fi
        if [ "n$mountdir" != "n$mountpath" ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume does not have the expected mountpath, mountpath was $mountdir >> $logFile
        fi
        if [ $repairedVolume -ne 0 ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume cannot be re-used >> $logFile
            cat $commandOutputFile >> $logFile
        fi
        if [ $repfactor -ne 1 ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume does not have the expected replication factor >>$logFile
        fi
        if [ $rackpath -ne 1 -a $computeNode -ne 1 ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume does not have the expected rackpath and this is not a compute node >> $logFile
        fi
        if [ $notreadonly -ne 1 ]; then
            echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume is set to read-only >> $logFile
        fi
        echo `date +"%Y-%m-%d %T"` INFO Pre-existing volume does not have the expected state, removing it >> $logFile
        runCommandWithTimeout 0 60 1 1 "maprcli volume unmount -force 1 -name $vol"
        runCommandWithTimeout 0 60 1 1 "maprcli volume remove -force true -name $vol"
        runCommandWithTimeout 0 60 1 1 "maprcli volume link remove -path $mountpath"
        runCommandWithTimeout 0 60 1 1 "maprcli volume info -name $vol"
        if [ $? -eq 0 ]; then
          echo `date +"%Y-%m-%d %T"` FATAL Issued volume remove API but the volume still exists, exiting >> $logFile
          exit $VOLUME_REMOVAL_FAILED
        fi
        echo `date +"%Y-%m-%d %T"` INFO Removed the pre-existing volume  >> $logFile

        checkPathAndCreateNewTTVolume
    fi
else
    echo `date +"%Y-%m-%d %T"` INFO A pre-existing $LONG_NAME volume could not be found, will try to create one.  >> $logFile
    checkPathAndCreateNewTTVolume
fi

#At this point, the volume exists and is mounted at the expected path
echo `date +"%Y-%m-%d %T"` INFO Checking for pre-existing content in the $LONG_NAME volume >> $logFile
runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $fullpath"
if [ $? -eq 0 ]; then
    # Have pre-existing content, should we keep it?
    if [ $keepExistingContent -eq 0 ]; then
        echo `date +"%Y-%m-%d %T"` INFO There is pre-existing content in the $LONG_NAME volume, removing it. >> $logFile
        runCommandWithTimeout 1 180 3 1 "hadoop fs -mv $fullpath $mountpath/old.`date +%s`"
        nohup hadoop fs -rmr $mountpath/old.* > /dev/null 2> /dev/null &
        echo `date +"%Y-%m-%d %T"` INFO The pre-existing content is being deleted in the background by process $!  >> $logFile
        echo `date +"%Y-%m-%d %T"` INFO Creating directories in the $LONG_NAME volume that will be needed by the $LONG_NAME process >> $logFile
        runCommandWithTimeout 1 600 60 10 "hadoop fs -mkdir -p $fullpath"
    else
        echo `date +"%Y-%m-%d %T"` INFO Verifying directories in the pre-existing $LONG_NAME volume >> $logFile
        # Delete dummy file created at TT start to find shuffleRootFid ips
        runCommandWithTimeout 0 180 1 1 "hadoop fs -rmr $fullpath/fidservers"
    fi
else
    echo `date +"%Y-%m-%d %T"` INFO Found no pre-existing content in the $LONG_NAME volume. Creating directories that will be needed by the $LONG_NAME process >> $logFile
    runCommandWithTimeout 1 600 60 10 "hadoop fs -mkdir -p $fullpath"
fi

# removing lock
UnlockSingleInstance

runCommandWithTimeout 1 180 3 1 "hadoop mfs -setcompression on $fullpath"

if [ "$4" == "staging" ] ; then
    #Done with NM staging, no need to create mapreduce shuffle structure in this volume
    echo `date +"%Y-%m-%d %T"` INFO The $LONG_NAME local volume has been setup successfully. >> $logFile
    exit $SUCCESS
fi
runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $fullpath/spill"
if [ $? -ne 0 ]; then
    runCommandWithTimeout 1 180 3 1 "hadoop fs -mkdir $fullpath/spill"
fi
runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $fullpath/output"
if [ $? -ne 0 ]; then
    runCommandWithTimeout 1 180 3 1 "hadoop fs -mkdir $fullpath/output"
fi
runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $fullpath/spill.U"
if [ $? -ne 0 ]; then
    runCommandWithTimeout 1 180 3 1 "hadoop fs -mkdir $fullpath/spill.U"
fi
runCommandWithTimeout 0 60 1 1 "hadoop fs -stat $fullpath/output.U"
if [ $? -ne 0 ]; then
    runCommandWithTimeout 1 180 3 1 "hadoop fs -mkdir $fullpath/output.U"
fi
runCommandWithTimeout 1 180 3 1 "hadoop mfs -setcompression off $fullpath/spill.U"
runCommandWithTimeout 1 180 3 1 "hadoop mfs -setcompression off $fullpath/output.U"

echo `date +"%Y-%m-%d %T"` INFO The $LONG_NAME local volume has been setup successfully. >> $logFile
exit $SUCCESS