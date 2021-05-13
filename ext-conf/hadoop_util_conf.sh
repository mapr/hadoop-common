#!/bin/bash
HADOOP_HOME=${HADOOP_HOME:-__INSTALL__}
MAPR_HOME=${MAPR_HOME:-__PREFIX__}
HADOOP_SYMLINKS_SCRIPT=${HADOOP_HOME}/bin/hadoop_symlinks.sh
HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
HADOOP_SSL_CLIENT_FILE=${HADOOP_CONF_DIR}/ssl-client.xml
HADOOP_SSL_SERVER_FILE=${HADOOP_CONF_DIR}/ssl-server.xml

function ConfigureSSL() {
    if [ -f "$HADOOP_HOME/etc/hadoop/.not_configured_yet" ]; then
        prevConf=`find ${MAPR_HOME}/hadoop -regextype posix-extended -regex '^.*hadoop-([0-9])\.([0-9])\.([0-9])\.([0-9]+)\.([0-9]+)' | sort -rV | head -n1`
        if [ ! -z "$prevConf" ]; then
            cp $prevConf/etc/hadoop/ssl-client.xml $HADOOP_SSL_CLIENT_FILE
            cp $prevConf/etc/hadoop/ssl-server.xml $HADOOP_SSL_SERVER_FILE
        fi
    fi
    sed -i -e 's|{MAPR_HOME}|'${MAPR_HOME}'|g' "$HADOOP_SSL_CLIENT_FILE"
    sed -i -e 's|{MAPR_HOME}|'${MAPR_HOME}'|g' "$HADOOP_SSL_SERVER_FILE"
    chmod 640 "$HADOOP_SSL_SERVER_FILE"

    ln -sf "$HADOOP_SSL_CLIENT_FILE" "${MAPR_CONF_DIR}"/ssl-client.xml
    ln -sf "$HADOOP_SSL_SERVER_FILE" "${MAPR_CONF_DIR}"/ssl-server.xml
    chmod 640 "${MAPR_CONF_DIR}"/ssl-server.xml
}


#Execute configuration scripts for hadoop util package
$HADOOP_SYMLINKS_SCRIPT
ConfigureSSL

# remove state file
if [ -f "$HADOOP_HOME/etc/hadoop/.not_configured_yet" ]; then
    rm -f "$HADOOP_HOME/etc/hadoop/.not_configured_yet"
fi
