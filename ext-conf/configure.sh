#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#############################################################################
#
# Script to configure hadoop componenbts
#
# __INSTALL_ (double underscore at the end)  gets expanded to __INSTALL__ during pakcaging
# set HADOOP_HOME explicitly if running this in a source built env.
#
# This script is sourced from the master configure.sh, this way any variables
# we need are available to us.
#
# It also means that this script should never do an exit in the case of failure
# since that would cause the master configure.sh to exit too. Simply return
# an return value if needed. Sould be 0 for the most part.
#
# When called from the master installer, expect to see the following options:
#

# This gets fillled out at package time
HADOOP_HOME="${HADOOP_HOME:-__INSTALL__}"
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
HADOOP_SSL_CLIENT_FILE=${HADOOP_CONF_DIR}/ssl-client.xml
HADOOP_SSL_SERVER_FILE=${HADOOP_CONF_DIR}/ssl-server.xml
MAPR_HOME=${MAPR_HOME:-__PREFIX__}
HADOOP_BASE="${MAPR_HOME}/hadoop"
NOW=$(date "+%Y%m%d_%H%M%S")
WARDEN_START_KEY="service.command.start"
WARDEN_HEAPSIZE_MIN_KEY="service.heapsize.min"
WARDEN_HEAPSIZE_MAX_KEY="service.heapsize.max"
WARDEN_HEAPSIZE_PERCENT_KEY="service.heapsize.percent"
WARDEN_RUNSTATE_KEY="service.runstate"
MAPR_CLUSTRERS_CONF_FILE=${MAPR_HOME}/conf/mapr-clusters.conf
RC=0
hadoop=2
hadoopVersion="__VERSION_3DIGIT__"
yarn_version="${hadoopVersion}"
isOnlyRoles=0

if [ -e "${MAPR_HOME}/server/common-ecosystem.sh" ]; then
    . "${MAPR_HOME}/server/common-ecosystem.sh"
else
    echo "Failed to source common-ecosystem.sh"
    exit 0
fi

INSTALL_DIR=${MAPR_HOME}
SERVER_DIR=${INSTALL_DIR}/server
. ${SERVER_DIR}/configure-common.sh

# Gloabal variables initialized in main method
HADOOP_VERSION=
RM_IP=
TL_IP=
HS_IP=

RM_RESTART_FILE="${RESTART_DIR}/resourcemanager_restart.sh"
TL_RESTART_FILE="${RESTART_DIR}/timelineserver_restart.sh"

############## functions

# The variables $yarn_version, etc. are obtained from hadoop_versions file
# that is sourced in this script.
function checkIncompatibleHadoopConfig() {

    if [ -f "${MAPR_HOME}/conf/hadoop_version" ]; then
        source "${MAPR_HOME}/conf/hadoop_version"
        if [ "$default_mode" = "classic" ]; then
            errMsg="Classic Hadoop configuration no longer supported"
            echo $errMsg
            logErr $errMsg
            exit 1
        elif [ "$default_mode" != "yarn" ]; then
            errMsg="Unknown Hadoop configuration - $default_mode"
            exit 1
        fi
    fi
}

function isSecureEnable() {
    local  __resultvar=$1
    local result=$(head -1 ${MAPR_CLUSTRERS_CONF_FILE} | grep -o 'secure=\w*' | cut -d= -f2)
    eval $__resultvar="'$result'"
}

function UpdateMapredSiteXmlForTezSSL() {
     isSecureEnable isSecure
     if [ "$isSecure" == "true" ] || [ "$secureCluster" == "1" ]; then
        if ! grep -q mapreduce.shuffle.ssl.enabled $hmrConf; then
            sed -i -e "s|</configuration>|  <property>\n    <name>mapreduce.shuffle.ssl.enabled</name>\n    <value>true</value>\n    </property>\n</configuration>|" $hmrConf
        else
            key="<name>mapreduce.shuffle.ssl.enabled<\/name>"
            value="true"
            sed -i -e '/'"$key"'/{N
            s/\('"$key"' *\n* *<value>\)\(.*\)\(<\/value>\)/\1'"$value"'\3/}' "$hmrConf"
        fi
    fi
}


function UpdateFileClientConfig() {
    # TODO: figure out if this stays here

    logInfo "Updating file client config"
    #edit core-site.xml file to make file clients default to <cldb-ip>
    key="<name>fs.default.name<\/name>"
    value="maprfs\:\/\/\/"
    sed -i -e '/'"$key"'/{
    N
    s/\('"$key"' *\n* *<value>\)\(.*\)\(<\/value>\)/\1'"$value"'\3/
  }' "$hcoreConf"

    # edit core-site.xml to make sure the correct user is set

    if ! grep -q hadoop.proxyuser.$MAPR_USER.hosts $hcoreConf; then
        sed -i -e "s|</configuration>|  <property>\n    <name>hadoop.proxyuser.$MAPR_USER.hosts</name>\n    <value>*</value>\n    <description>The superuser $MAPR_USER can connect from any host to impersonate a user</description>\n  </property>\n</configuration>|" $hcoreConf
    fi

    if ! grep -q hadoop.proxyuser.$MAPR_USER.groups $hcoreConf; then
        sed -i -e "s|</configuration>|  <property>\n    <name>hadoop.proxyuser.$MAPR_USER.groups</name>\n    <value>*</value>\n    <description>Allow the superuser $MAPR_USER to impersonate any member of any group</description>\n  </property>\n</configuration>|" $hcoreConf
    fi

    key="<name>hadoop\.proxyuser\.mapr\.groups<\/name>"
    replacementKey="<name>hadoop\.proxyuser\.$MAPR_USER\.groups<\/name>"
    sed -i -e 's/'"$key"'/'"$replacementKey"'/' $hcoreConf

    key="<name>hadoop\.proxyuser\.mapr\.hosts<\/name>"
    replacementKey="<name>hadoop\.proxyuser\.$MAPR_USER\.hosts<\/name>"
    sed -i -e 's/'"$key"'/'"$replacementKey"'/' $hcoreConf

    # edit fair-scheduler.xml to add Acls
    if [ -n "$fairSchedulerConf" ] && ! grep -q "aclSubmitApps" "$fairSchedulerConf"; then
        sed -i -e "s|</allocations>|  <queue name=\"root\">\n    <aclSubmitApps>*</aclSubmitApps>\n    <aclAdministerApps> </aclAdministerApps>\n  </queue>\n</allocations>|" "$fairSchedulerConf"
    fi

    # edit capacity-scheduler.xml to add Acls
    if [ -n "$capacitySchedulerConf" ] && ! grep -q "yarn.scheduler.capacity.root.acl_submit_applications" "$capacitySchedulerConf"; then
        sed -i -e "s|</configuration>|  <property>\n   <name>yarn.scheduler.capacity.root.acl_submit_applications</name>\n    <value>*</value>\n    <description>\n      The ACL of who can submit jobs to the root queue.\n    </description>\n  </property>\n</configuration>|" "$capacitySchedulerConf"
    fi

    if [ -n "$capacitySchedulerConf" ] && ! grep -q "yarn.scheduler.capacity.root.acl_administer_queue" "$capacitySchedulerConf"; then
        sed -i -e "s|</configuration>|  <property>\n   <name>yarn.scheduler.capacity.root.acl_administer_queue</name>\n    <value> </value>\n    <description>\n      The ACL of who can administer jobs on the root queue.\n    </description>\n  </property>\n</configuration>|" "$capacitySchedulerConf"
    fi

    # change value for root.default.acl_administer_queue to space
    if [ -n "$capacitySchedulerConf" ] && grep -q "yarn.scheduler.capacity.root.default.acl_administer_queue" "$capacitySchedulerConf"; then
        sed -i -e "/yarn.scheduler.capacity.root.default.acl_administer_queue/{n;s|\(<value>\).*\(</value>\)|\1 \2|;}" "$capacitySchedulerConf"
    fi

    # tell mapreduce to use maprfs
    key="<name>mapreduce.use.maprfs<\/name>"
    value="true"
    sed -i -e '/'"$key"'/{
    N
    s/\('"$key"' *\n* *<value>\)\(.*\)\(<\/value>\)/\1'"$value"'\3/
  }' "$hmrConf"

    if [ "$hadoopVersion" == "trunk" ]; then
        key="<name>mapreduce.jobtracker.address<\/name>"
    else
        key="<name>mapred.job.tracker<\/name>"
    fi
    value="maprfs\:\/\/\/"
    sed -i -e '/'"$key"'/{
    N
    s/\('"$key"' *\n* *<value>\)\(.*\)\(<\/value>\)/\1'"$value"'\3/
  }' "$hmrConf"

    UpdateMapredSiteXmlForTezSSL
}
function ConfigureCommon() {
    # Remove old maprfs jars.
    cleanUpOldMapRfsJars ".*maprfs-[[:digit:]].*\.jar"
    cleanUpOldMapRfsJars ".*maprfs-jni-[[:digit:]].*\.jar"
    cleanUpOldMapRfsJars ".*maprfs-core-[[:digit:]].*\.jar"
    cleanUpOldMapRfsJars ".*mysql-container-java-[[:digit:]].*\.jar"
}

function ConfigureYarnSiteXml() {
    local phatJar=""
    local FILENAME="yarn-site"
    local FILE="${HADOOP_HOME}/etc/hadoop/${FILENAME}.xml"
    local TEMP_FILE="${HADOOP_HOME}/etc/hadoop/${FILENAME}.xml.tmp"
    local TIMESTAMP="$(date +%F.%H-%M)"
    phatJar="$(ls $INSTALL_DIR/lib/hadoop-yarn-common-*.jar | grep -v jni | grep -v diagnostic | grep -v core | grep -v test)"
    if [ -z "$phatJar" ] || [ ! -f "$phatJar" ]; then
        logErr "Failed to find hadoop-yarn-common jar"
        exit 1
    fi
    if [ -f ${FILE} ]; then
        logInfo "Backing up \"$HADOOP_HOME/etc/hadoop/yarn-site.xml\" to \"$HADOOP_HOME/etc/hadoop/yarn-site-${TIMESTAMP}.xml\""
        cp ${FILE} $HADOOP_HOME/etc/hadoop/${FILENAME}-${TIMESTAMP}.xml
    fi

    $HADOOP_HOME/bin/hadoop jar $phatJar $@ $HADOOP_HOME/etc/hadoop/yarn-site.xml >$TEMP_FILE

    if [ $? -ne 0 ]; then
        echo "ERROR configuring yarn-site.xml."
        exit 1
    fi
    mv ${TEMP_FILE} ${FILE}
}

function ConfigureHS() {
    # Default HS_IP to RM_IP if not defined and if it's the first time running this section
    # It will only set HS_IP to RM_IP IF __HS_IP__ is found in mapred-site.xml
    if [ -z "${HS_IP}" -a $(grep "__HS_IP__" "${HADOOP_HOME}/etc/hadoop/mapred-site.xml" | wc -l) -ne 0 ]; then
        logInfo "No IP/hostname provided for History Server. Will be configured to 0.0.0.0"
        HS_IP="0.0.0.0"
    fi

    # Set history server IP in yarn-site.xml
    if [ ! -z "$HS_IP" ]; then
        FILENAME="mapred-site"
        FILE="${HADOOP_HOME}/etc/hadoop/${FILENAME}.xml"
        TMPL="${FILE}.template"
        # Check if old file has HS set already. If it does, then back it up
        if [ $(grep "__HS_IP__" $FILE | wc -l) -eq 0 ]; then
            TIMESTAMP=$(date +%F.%H-%M)
            logInfo "Backing up \"$HADOOP_HOME/etc/hadoop/mapred-site.xml\" to \"$HADOOP_HOME/etc/hadoop/mapred-site-${TIMESTAMP}.xml\""
            cp ${FILE} $HADOOP_HOME/etc/hadoop/${FILENAME}-${TIMESTAMP}.xml
        fi
        # Replace HS_IP from template file and redirect output to new file
        sed "s/__HS_IP__/${HS_IP}/g" "${TMPL}" >"${FILE}"
    fi
}

function IsRMHAConfiguration() {
    local rmIPs=$1
    local prevIFS=$IFS
    set -- "$rmIPs"
    IFS=","
    declare -a RM_IP_ARRAY=($*)
    IFS=$prevIFS
    if [ ${#RM_IP_ARRAY[@]} -eq 1 ]; then
        return 1
    fi
    return 0
}

function CreateRMRestartFile() {
    if ! [ -d "${RESTART_DIR}" ]; then
        mkdir -p "${RESTART_DIR}"
    fi
    if [ -f $ROLES/resourcemanager ]; then
        if ! [ -f "${RM_RESTART_FILE}" ]; then
            cat >"$RM_RESTART_FILE" <<-RM_RESTART
              echo "Running RM restart script"
              if ${MAPR_HOME}/initscripts/mapr-warden status > /dev/null 2>&1 ; then
                  isSecure=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
                  if [ "$isSecure" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
                      export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
                  fi
                  nohup maprcli node services -name resourcemanager -action restart -nodes $(hostname -f) > ${RESTART_LOG_DIR}/rm_restart_$(date +%s)_$$.log 2>&1 &
              fi
RM_RESTART
            chmod +x "$RM_RESTART_FILE"
        fi
    fi
}

function ConfigureWardenRMHA() {

    # Configure RM Service in Warden to run only on a single node or multiple nodes depending on
    # whether MapR RM HA is configured or not.
    local WardenRMConfFile=$1
    local maprHA=$2

    if [ -e ${WardenRMConfFile} ]; then
        if [[ $maprHA -eq 1 ]]; then
            runOnNodes=1
        else
            runOnNodes=all
        fi
        sed -i -e "s/^services=resourcemanager:.*:cldb$/services=resourcemanager:${runOnNodes}:cldb/" ${WardenRMConfFile}
    fi
}

function ConfigureYarnServices() {
    maprHA=0
    # Set Resource Manager IP
    if [ ! -z "$1" ]; then
        RM_IP="$1"
    else
        maprHA=1
    fi

    if [ ! -z "$2" ]; then
        HS_IP="$2"
    fi

    if [ $maprHA -eq 1 ]; then
        logInfo "No RM addresses were provided. Will configure MapR HA for Resource Manager.."
        ConfigureYarnSiteXml org.apache.hadoop.yarn.configuration.YarnSiteMapRHAXmlBuilder
    else
        IsRMHAConfiguration $RM_IP
        if [ $? -eq 0 ]; then
            logInfo "Multiple IPs/hostnames are provided for Resource Manager. Will configure high availability (HA) for Resource Manager.."
            clusterName=$(getClusterName)
            zkNodesList=$(getZKServers)
            ConfigureYarnSiteXml org.apache.hadoop.yarn.configuration.YarnHASiteXmlBuilder ${RM_IP} $clusterName $zkNodesList
        else
            ConfigureYarnSiteXml org.apache.hadoop.yarn.configuration.YarnSiteXmlBuilder ${RM_IP}
        fi
    fi

    isSecureEnable MAPR_SECURITY_STATUS
    local yarnSiteChange=0
    local YarnSiteFile="${HADOOP_HOME}/etc/hadoop/yarn-site.xml"
    if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
        ConfigureYarnSiteXml org.apache.hadoop.yarn.configuration.YarnSiteAclXmlBuilder

        if [ "$MAPR_USER" != "mapr" ]; then
            local tmpFile="/tmp/rmp.$$"
            if ! grep -Fq 'yarn.resourcemanager.principal' "$YarnSiteFile"; then
                echo -e "  <!--RM PRINCIPAL SECTION-->\n  <property>\n    <name>yarn.resourcemanager.principal</name>\n    <value>$MAPR_USER</value>\n  </property>\n  <!--RM PRINCIPAL SECTION END-->" >"$tmpFile"
                sed -i -e "/<\/configuration>/ {
                    r ${tmpFile}
                    d
                    }" ${YarnSiteFile}
                echo "</configuration>" >>${YarnSiteFile}
                rm -f $tmpFile
                yarnSiteChange=1
            else
                if !( grep -FA 1 'yarn.resourcemanager.principal' "$YarnSiteFile" | grep -q $MAPR_USER ); then
                    sed -i -e "/<name>yarn.resourcemanager.principal<\/name>/!b;n;c\ \ \ \ <value>$MAPR_USER<\/value>" ${YarnSiteFile}
                    yarnSiteChange=1
                fi
            fi
        else
            if (grep -Fq "<!--RM PRINCIPAL SECTION-->" ${YarnSiteFile}); then
                yarnSiteChange=1
                sed -i -e '/<!--RM PRINCIPAL SECTION-->/,/<!--RM PRINCIPAL SECTION END-->/d' ${YarnSiteFile}
            fi
        fi
    else
        if (grep -Fq "<!--RM PRINCIPAL SECTION-->" ${YarnSiteFile}); then
            yarnSiteChange=1
            sed -i -e '/<!--RM PRINCIPAL SECTION-->/,/<!--RM PRINCIPAL SECTION END-->/d' ${YarnSiteFile}
        fi
    fi

    if [ $yarnSiteChange -eq 1 ] && ${MAPR_HOME}/initscripts/mapr-warden status >/dev/null 2>&1; then
        CreateRMRestartFile
    fi

    ConfigureHS
}

function ConfigureTimeLineServer() {
    local YarnSiteFile="${HADOOP_HOME}/etc/hadoop/yarn-site.xml"
    local YarnTLProps="${HADOOP_HOME}/etc/hadoop/yarn-timelineserver-properties.xml"
    local YarnTLSecurityProps="${HADOOP_HOME}/etc/hadoop/yarn-timelineserver-security-properties.xml"
    local YSTIMESTAMP=$(date +%F.%H-%M)
    local yarnSiteChange=0
    isSecureEnable isSecure
    if [ -f ${YarnSiteFile} ]; then
        logInfo "Backing up \"$HADOOP_HOME/etc/hadoop/yarn-site.xml\" to \"$HADOOP_HOME/etc/hadoop/yarn-site-${YSTIMESTAMP}.xml\""
        cp ${YarnSiteFile} $HADOOP_HOME/etc/hadoop/yarn-site-${YSTIMESTAMP}.xml
    fi

    #add timeline-server properties to yarn-site.xml
    if !(grep -Fq "<!--TIMELINE SERVER SECTION-->" ${YarnSiteFile}); then
        yarnSiteChange=1
        sed -i -e "/<\/configuration>/ {
              r ${YarnTLProps}
              d
              }" ${YarnSiteFile}
        echo "</configuration>" >> ${YarnSiteFile}
    fi
    if (grep -Fq "<!--TIMELINE SERVER SECTION-->" ${YarnSiteFile}); then
        sed -i -e "/<name>yarn.timeline-service.hostname<\/name>/!b;n;c\ \ \ \ <value>$1<\/value>" ${YarnSiteFile}
    fi
    if [ "$isSecure" = "true" ]; then
        if !(grep -Fq "<!--TIMELINE SECURITY SECTION-->" ${YarnSiteFile}); then
            yarnSiteChange=1
            sed -i -e "/<\/configuration>/ {
                  r ${YarnTLSecurityProps}
                  d
                  }" ${YarnSiteFile}
            echo "</configuration>" >> ${YarnSiteFile}
        fi
    fi

    if [ "$isSecure" = "false" ]; then
        if (grep -Fq "<!--TIMELINE SECURITY SECTION-->" ${YarnSiteFile}); then
            yarnSiteChange=1
            sed -i -e '/<!--TIMELINE SECURITY SECTION-->/,/<!--TIMELINE SECURITY SECTION END-->/d' ${YarnSiteFile}
        fi
    fi

    if [ $yarnSiteChange -eq 1 ] && ${MAPR_HOME}/initscripts/mapr-warden status >/dev/null 2>&1; then
        CreateRMRestartFile
        if [ -f $ROLES/timelineserver ]; then
            if ! [ -f "${TL_RESTART_FILE}" ]; then
                cat >"$TL_RESTART_FILE" <<-TL_RESTART
                  echo "Running TL restart script"
                  if ${MAPR_HOME}/initscripts/mapr-warden status > /dev/null 2>&1 ; then
                      isSecure=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
                      if [ "$isSecure" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
                          export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
                      fi
                      nohup maprcli node services -name timelineserver -action restart -nodes $(hostname -f) > ${RESTART_LOG_DIR}/tl_restart_$(date +%s)_$$.log 2>&1 &
                  fi
TL_RESTART
                chmod +x "$TL_RESTART_FILE"
            fi
        fi
    fi
}

function ConfigureHadoop2() {
    sed -i -e 's|{MAPR_HOME}|'${MAPR_HOME}'|g' "$HADOOP_SSL_CLIENT_FILE"
    sed -i -e 's|{MAPR_HOME}|'${MAPR_HOME}'|g' "$HADOOP_SSL_SERVER_FILE"
    chmod 640 "$HADOOP_SSL_SERVER_FILE"

    ln -sf "$HADOOP_SSL_CLIENT_FILE" "${MAPR_CONF_DIR}"/ssl-client.xml
    ln -sf "$HADOOP_SSL_SERVER_FILE" "${MAPR_CONF_DIR}"/ssl-server.xml
    chmod 640 "${MAPR_CONF_DIR}"/ssl-server.xml
}

function ConfigureYarnLinuxContainerExecutor() {
    # Only configure container executor for yarn
    # Set the MapR specific values in container-executor.cfg
    FILENAME="container-executor.cfg"
    FILE=${HADOOP_HOME}/etc/hadoop/${FILENAME}
    sed -i -e "s/^\(yarn\.nodemanager\.linux-container-executor\.group\)=#.*$/\1=${MAPR_GROUP}/" ${FILE}
    sed -i -e "s/^\(min\.user\.id\)=1000#.*$/\1=500/" ${FILE}
    sed -i -e "s/^\(allowed\.system\.users\)=#.*$/\1=${MAPR_USER}/" ${FILE}

    # Change ownership and mode for container-executor binary.
    chown root:${MAPR_GROUP} ${HADOOP_HOME}/bin/container-executor
    chmod 6050 ${HADOOP_HOME}/bin/container-executor
}

function cleanUpOldMapRfsJars() {
    local ITEM
    local LATEST=""

    for ITEM in $(find $HADOOP_HOME/lib -regextype posix-extended -regex "$1" -print 2>/dev/null); do
        if [ -z "$LATEST" ]; then
            LATEST="$ITEM"
        elif [ "$ITEM" -nt "$LATEST" ]; then
            rm -f "$LATEST"
            LATEST="$ITEM"
        elif [ "$ITEM" -ot "$LATEST" ]; then
            rm -f "$ITEM"
        fi
    done
}

function ConfigureHadoopMain() {
    # Process arguments
    if [ -z "$1" ]; then
        HADOOP=2
    else
        HADOOP="$1"
    fi

    if [ -z "$2" ]; then
        HADOOP_VERSION=$(cat $MAPR_HOME/hadoop/hadoopversion)
    else
        HADOOP_VERSION="$2"
    fi

    ConfigureCommon

    ConfigureHadoop2
}

function ConfigureHadoop() {
    if [ ! -d "$HADOOP_HOME" ]; then
        logInfo "Skipping Hadoop configuration... Not found"
        return
    fi

    ConfigureRunUserForHadoop $MAPR_USER

    hConf="${HADOOP_CONF_DIR}/hadoop-site.xml"
    hcoreConf="${HADOOP_CONF_DIR}/core-site.xml"
    hmrConf="${HADOOP_CONF_DIR}/mapred-site.xml"
    fairSchedulerConf="${HADOOP_CONF_DIR}/fair-scheduler.xml"
    capacitySchedulerConf="${HADOOP_CONF_DIR}/capacity-scheduler.xml"
}

function ConfigureJMHadoopProperties() {
    file=$1
    grep "maprmepredvariant.class" $file >/dev/null 2>&1
    if [ "$?" -ne 0 ]; then
        # insert record
        echo "maprmepredvariant.class=com.mapr.job.mngmnt.hadoop.metrics.MaprRPCContext" >>$file
    else
        # update record
        sed -i -e 's/^maprmepredvariant.class=.*$/maprmepredvariant.class=com.mapr.job.mngmnt.hadoop.metrics.MaprRPCContext/g' $file
    fi

    grep "maprmepredvariant.period" $file >/dev/null 2>&1
    if [ "$?" -ne 0 ]; then
        # insert record
        echo "maprmepredvariant.period=10" >>$file
    fi

    grep "maprmapred.class" $file >/dev/null 2>&1
    if [ "$?" -ne 0 ]; then
        # insert record
        echo "maprmapred.class=com.mapr.job.mngmnt.hadoop.metrics.MaprRPCContextFinal" >>$file
    else
        # update record
        sed -i -e 's/^maprmapred.class=.*$/maprmapred.class=com.mapr.job.mngmnt.hadoop.metrics.MaprRPCContextFinal/g' $file
    fi

    grep "maprmapred.period" $file >/dev/null 2>&1
    if [ "$?" -ne 0 ]; then
        # insert record
        echo "maprmapred.period=10" >>$file
    fi

}

# Sets up symlinks, updates configuration files. This is different from
# ConfigureHadoop. It needs to be run only when the user wants to specify
# a new Hadoop version to be configured. It is not needed when roles are
# refreshed.
function ConfigureHadoopDir() {
    ConfigureHadoopMain "$hadoop" "$hadoopVersion"
}

function ConfigureRunUserForHadoopInternal() {
    HADOOP_DIR="${INSTALL_DIR}/hadoop/hadoop-${2}"
    if [ ! -d $HADOOP_DIR ]; then
        logWarn "Hadoop directory does not exist: $HADOOP_DIR"
        return
    fi

    CURR_USER=$1
    [ -d "${HADOOP_DIR}/logs" ] && chown $CURR_USER "${HADOOP_DIR}/logs" >>$logFile 2>&1
    [ -d "${HADOOP_DIR}/logs" ] && [ "$(ls -A ${HADOOP_DIR}/logs)" ] && chown $CURR_USER "${HADOOP_DIR}/logs/"* >>$logFile 2>&1
    [ -d "${HADOOP_DIR}/pids" ] && chown -R $CURR_USER "${HADOOP_DIR}/pids" >>$logFile 2>&1
    [ -d "${HADOOP_DIR}/conf" ] && chown -R $CURR_USER "${HADOOP_DIR}/conf" >>$logFile 2>&1
    [ -d "${HADOOP_DIR}/conf.new" ] && chown -R $CURR_USER "${HADOOP_DIR}/conf.new" >>$logFile 2>&1
    [ -d "${HADOOP_DIR}/etc/hadoop" ] && find "${HADOOP_DIR}/etc/hadoop" -type f | grep -v container-executor.cfg | xargs chown $CURR_USER >>$logFile 2>&1
}

function ConfigureRunUserForHadoop() {

    # Configure for Hadoop 2

    ConfigureRunUserForHadoopInternal $1 $yarn_version

}

function ConfigureJMXForHadoop() {
    #TODO
    # currently collectd's configure script does a bunch of seds into the yarn script to
    # enable JMX and set options and ports for JMX.
    #

    if hasRole "resourcemanager" ; then
        : # do what collect does for RM
    fi
    if hasRole "nodemanager" ; then
        : # do what collect does for NM
    fi
    if hasRole "historyserver" ; then
        : # do what collect does for HS
    fi
    if hasRole "timelineserver" ; then
        : # do what collect does for TL
    fi
}

#############################################################################
# Function to extract key from warden config file
#
# Expects the following input:
# $1 = warden file to extract key from
# $2 = the key to extract
#
#############################################################################
function get_warden_value() {
    local f=$1
    local key=$2
    local val=""
    local rc=0
    if [ -f "$f" ] && [ -n "$key" ]; then
        val=$(grep "$key" "$f" | cut -d'=' -f2 | sed -e 's/ //g')
        rc=$?
    fi
    echo "$val"
    return $rc
}

#############################################################################
# Function to update value for  key in warden config file
#
# Expects the following input:
# $1 = warden file to update key in
# $2 = the key to update
# $3 = the value to update with
#
#############################################################################
function update_warden_value() {
    local f=$1
    local key=$2
    local value=$3

    sed -i 's/\([ ]*'"$key"'=\).*$/\1'"$value"'/' "$f"
}

#############################################################################
# Function to install Warden conf file
#
#############################################################################
function installWardenConfFile() {
    local rc=0
    local curr_start_cmd
    local curr_heapsize_min
    local curr_heapsize_max
    local curr_heapsize_percent
    local curr_runstate
    local pkg_start_cmd
    local pkg_heapsize_min
    local pkg_heapsize_max
    local pkg_heapsize_percent
    local newestPrevVersionFile
    local tmpWardenFile
    local service_name=$1

    INST_WARDEN_FILE="${MAPR_CONF_CONFD_DIR}/warden.${service_name}.conf"
    PKG_WARDEN_FILE="${HADOOP_HOME}/ext-conf/warden.${service_name}.conf"

    tmpWardenFile=$(basename $PKG_WARDEN_FILE)
    tmpWardenFile="/tmp/${tmpWardenFile}$$"

    if [ -f "$INST_WARDEN_FILE" ]; then
        curr_start_cmd=$(get_warden_value "$INST_WARDEN_FILE" "$WARDEN_START_KEY")
        curr_heapsize_min=$(get_warden_value "$INST_WARDEN_FILE" "$WARDEN_HEAPSIZE_MIN_KEY")
        curr_heapsize_max=$(get_warden_value "$INST_WARDEN_FILE" "$WARDEN_HEAPSIZE_MAX_KEY")
        curr_heapsize_percent=$(get_warden_value "$INST_WARDEN_FILE" "$WARDEN_HEAPSIZE_PERCENT_KEY")
        curr_runstate=$(get_warden_value "$INST_WARDEN_FILE" "$WARDEN_RUNSTATE_KEY")
        pkg_start_cmd=$(get_warden_value "$PKG_WARDEN_FILE" "$WARDEN_START_KEY")
        pkg_heapsize_min=$(get_warden_value "$PKG_WARDEN_FILE" "$WARDEN_HEAPSIZE_MIN_KEY")
        pkg_heapsize_max=$(get_warden_value "$PKG_WARDEN_FILE" "$WARDEN_HEAPSIZE_MAX_KEY")
        pkg_heapsize_percent=$(get_warden_value "$PKG_WARDEN_FILE" "$WARDEN_HEAPSIZE_PERCENT_KEY")

        if [ "$curr_start_cmd" != "$pkg_start_cmd" ]; then
            cp "$PKG_WARDEN_FILE" "${tmpWardenFile}"
            if [ -n "$curr_runstate" ]; then
                echo "service.runstate=$curr_runstate" >>"${tmpWardenFile}"
            fi
            if [ -n "$curr_heapsize_min" ] && [ "$curr_heapsize_min" -gt "$pkg_heapsize_min" ]; then
                update_warden_value "${tmpWardenFile}" "$WARDEN_HEAPSIZE_MIN_KEY" "$curr_heapsize_min"
            fi
            if [ -n "$curr_heapsize_max" ] && [ "$curr_heapsize_max" -gt "$pkg_heapsize_max" ]; then
                update_warden_value "${tmpWardenFile}" "$WARDEN_HEAPSIZE_MAX_KEY" "$curr_heapsize_max"
            fi
            if [ -n "$curr_heapsize_percent" ] && [ "$curr_heapsize_percent" -gt "$pkg_heapsize_percent" ]; then
                update_warden_value "${tmpWardenFile}" "$WARDEN_HEAPSIZE_PERCENT_KEY" "$curr_heapsize_percent"
            fi
            cp "${tmpWardenFile}" "$INST_WARDEN_FILE"
            rc=$?
            rm -f "${tmpWardenFile}"
        fi
        ConfigureWardenRMHA "${INST_WARDEN_FILE}" "$maprHA"
    else
        if ! [ -d "${MAPR_CONF_CONFD_DIR}" ]; then
            mkdir -p "${MAPR_CONF_CONFD_DIR}" >/dev/null 2>&1
        fi
        newestPrevVersionFile=$(ls -t1 "$PKG_WARDEN_FILE"-[0-9]* 2>/dev/null | head -n 1)
        if [ -n "$newestPrevVersionFile" ] && [ -f "$newestPrevVersionFile" ]; then
            curr_runstate=$(get_warden_value "$newestPrevVersionFile" "$WARDEN_RUNSTATE_KEY")
            cp "$PKG_WARDEN_FILE" "${tmpWardenFile}"
            ConfigureWardenRMHA "${tmpWardenFile}" "$maprHA"
            if [ -n "$curr_runstate" ]; then
                echo "service.runstate=$curr_runstate" >>"${tmpWardenFile}"
            fi
            cp "${tmpWardenFile}" "$INST_WARDEN_FILE"
            rc=$?
            rm -f "${tmpWardenFile}"
        else
            cp "$PKG_WARDEN_FILE" "$INST_WARDEN_FILE"
            rc=$?
        fi
    fi
    if [ $rc -ne 0 ]; then
        logWarn "hadoop - Failed to install Warden conf file for service - service will not start"
    fi
    chown $MAPR_USER:$MAPR_GROUP "$INST_WARDEN_FILE"
}

#############################################################################
# Function to check and register port availablilty
#
#############################################################################
function registerPort() {
    local port=$1
    local name=$2
    if checkNetworkPortAvailability $port; then
        registerNetworkPort $name $port
        if [ $? -ne 0 ]; then
            logWarn "hadoop - Failed to register port $port for $name"
        fi
    else
        service=$(whoHasNetworkPort $port)
        if [ "$service" != "$name" ]; then
            logWarn "hadoop - port $port in use by $service service"
        fi
    fi
}

#############################################################################
# Function to check to make sure core is running
#
#############################################################################
function checkCoreUp() {
    local rc=0
    local svc=""
    local core_status_scripts="$MAPR_HOME/initscripts/mapr-warden"

    # only add the checks for services configured locally
    if [ -e "$MAPR_HOME/roles/zookeeper" ]; then
        core_status_scripts="$core_status_scripts $MAPR_HOME/initscripts/zookeeper"
    fi

    if [ -e "$MAPR_HOME/roles/cldb" ]; then
        core_status_scripts="$core_status_scripts $MAPR_HOME/initscripts/mapr-cldb"
    fi

    # make sure sercices are up
    for svc in $core_status_scripts; do
        $svc status
        rc=$?
        [ $rc -ne 0 ] && break
    done
    return $rc
}

# MAPRYARN-212
function checkTCFileForNodManager() {
    tc_path=/sbin/tc

    if [[ ! -f  $tc_path ]]; then
        if [ -f "/usr/sbin/tc" ]; then
            ln -s /usr/sbin/tc $tc_path
        else
             logWarn "Hadoop: Warning - /sbin/tc don't  exist, so you should create link ln -s file /sbin/tc, where file is path to the tc. (Need for cgroups! )"
        fi
    fi
}

# typically called from master configure.sh with the following arguments
#
# configure.sh  ....
#
# we need will use the roles file to know if this node is a RM. If this RM
# is not the active one, we will be getting 0s for the stats.
#

#sets MAPR_USER/MAPR_GROUP/logfile
#initialize the common library
initCfgEnv

# Parse the arguments
usage="usage: $0 [-help] [-EC <commonEcoOpts>] [-customSecure] [-secure] [-unsecure] [-R]"
if [ ${#} -gt 0 ]; then
    # we have arguments - run as as standalone - need to get params and
    OPTS=$(getopt -a -o chsuz:C: -l EC: -l help -l R -l customSecure -l unsecure -l secure -- "$@")
    if [ $? != 0 ]; then
        echo -e ${usage}
        return 2 2>/dev/null || exit 2
    fi
    eval set -- "$OPTS"

    while (($#)); do
        case "$1" in
            --EC | -C)
                #Parse Common options
                #Ingore ones we don't care about
                ecOpts=($2)
                shift 2
                restOpts="$@"
                eval set -- "${ecOpts[@]} --"
                while (($#)); do
                    case "$1" in
                        --OT | -OT)
                            nodelist="$2"
                            shift 2
                            ;;
                        --R | -R)
                            HADOOP_CONF_ASSUME_RUNNING_CORE=1
                            isOnlyRoles=1
                            shift 1
                            ;;
                        --RM | -RM)
                            rm_ip=$2
                            shift 2
                            ;;
                        --HS | -HS)
                            hs_ip=$2
                            shift 2
                            ;;
                        --TL | -TL)
                            tl_ip=$2
                            shift 2
                            ;;
                        --noStreams | -noStreams)
                            useStreams=0
                            shift
                            ;;
                        --)
                            shift
                            ;;
                        *)
                            #echo "Ignoring common option $j"
                            shift 1
                            ;;
                    esac
                done
                shift 2
                eval set -- "$restOpts"
                ;;
            --R | -R)
                HADOOP_CONF_ASSUME_RUNNING_CORE=1
                isOnlyRoles=1
                shift 1
                ;;
            --customSecure | -c)
                if [ -f "$OTSDB_HOME/etc/.not_configured_yet" ]; then
                    # hadoop added after secure 5.x cluster upgraded to customSecure
                    # 6.0 cluster. Deal with this by assuming a regular --secure path
                    :
                else
                    # this is a little tricky. It either means a simpel configure.sh -R run
                    # or it means that hadoop was part of the 5.x to 6.0 upgrade
                    # At the moment hadoop knows of no other security settings besides jmx
                    # and port numbers the jmx uses. Since we have no way of detecting what
                    # these ports are - we assume for now they don't change.
                    :
                fi
                secureCluster=1
                shift 1
                ;;
            --secure | -s)
                secureCluster=1
                shift 1
                ;;
            --unsecure | -u)
                secureCluster=0
                shift 1
                ;;
            --help | -h)
                echo -e ${usage}
                return 2 2>/dev/null || exit 2
                ;;
            --)
                shift
                ;;
            *)
                echo "Unknown option $1"
                echo -e ${usage}
                return 2 2>/dev/null || exit 2
                ;;
        esac
    done
fi

if [ -z "$zk_nodelist" ]; then
    zk_nodelist=$(getZKServers)
fi

# save off a copy of existing config file(s) - this I believe is partly anway handled by some of the java jars - verify there are no others- FIXME
#cp -p ${HADOOP_CONF_FILE} ${HADOOP_CONF_FILE}.${NOW}

# create new config file(s) - this I believe is partly anway handled by some of the java jars - verify there are no others- FIXME
#cp ${HADOOP_CONF_FILE} ${NEW_HADOOP_CONF_FILE}

#Do something here

# check if this is a HA configuration
if [ ! -z "$rm_ip" ]; then
    IsRMHAConfiguration $rm_ip
    if [ $? -eq 0 -a -z "$hs_ip" ]; then
        logErr "Hadoop: Error - No IP/hostname provided for History Server (-HS option). Exiting.."
        exit 1
    fi
fi

# check to see if we have old hadoop config
checkIncompatibleHadoopConfig

# check is tc path correct. Need for cgroups at the NodeManager
checkTCFileForNodManager

if [ ! -z "$rm_ip" ]; then
    ConfigureYarnServices "$rm_ip" "$hs_ip"
elif [ ! -f ${HADOOP_HOME}/etc/hadoop/yarn-site.xml ] || [ "$isOnlyRoles" != "1" ]; then
    # No -RM provided and no -R. Configure MapR-HA for RM.
    ConfigureYarnServices "" "$hs_ip"
fi
if [ ! -z "$tl_ip" ]; then
    ConfigureTimeLineServer "$tl_ip"
fi

#Always configure hadoop dir
ConfigureHadoopDir
ConfigureHadoop
UpdateFileClientConfig
ConfigureJMHadoopProperties "${INSTALL_DIR}/conf/hadoop-metrics.properties"
ConfigureYarnLinuxContainerExecutor

# TODO - this one is incomplete
ConfigureJMXForHadoop

if hasRole "nodemanager"; then
    installWardenConfFile nodemanager
fi
if hasRole "resourcemanager"; then
    installWardenConfFile resourcemanager
fi
if hasRole "historyserver"; then
    installWardenConfFile historyserver
fi
if hasRole "timelineserver"; then
    installWardenConfFile timelineserver
fi

# remove state file
if [ -f "$HADOOP_HOME/etc/.not_configured_yet" ]; then
    rm -f "$HADOOP_HOME/etc/.not_configured_yet"
fi

true
