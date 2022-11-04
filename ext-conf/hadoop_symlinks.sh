#!/bin/bash

# source MAPR_HOME/conf/env.sh to inherit MapR's env variables into 'bin/hadoop' script.
BASEMAPR=${MAPR_HOME:-__PREFIX_INSTALL__}
env=${BASEMAPR}/conf/env.sh
[ -f $env ] && . $env

function createSymlinks() {
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/ssl-client.xml ${MAPR_HOME}/conf/ssl-client.xml
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/ssl-server.xml ${MAPR_HOME}/conf/ssl-server.xml

  rm -f ${MAPR_HOME}/server/createRMVolume.sh
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/bin/createRMVolume.sh ${MAPR_HOME}/server/
  rm -f ${MAPR_HOME}/server/createLocalVolumes.sh
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/bin/createLocalVolumes.sh ${MAPR_HOME}/server/

  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/failureaccess-*
  ln -sf ${MAPR_HOME}/lib/failureaccess-* ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/guava-*
  ln -sf ${MAPR_HOME}/lib/guava-* ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/slf4j-api*
  ln -sf ${MAPR_HOME}/lib/slf4j-api* ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/slf4j-reload4j-* > /dev/null 2>&1
  ln -sf ${MAPR_HOME}/lib/slf4j-reload4j-*  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-hbase-*
  ln -sf ${MAPR_HOME}/lib/mapr-hbase-* ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mysql-connector-java-*.jar
  ln -sf ${MAPR_HOME}/lib/mysql-connector-java-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprfs-*.jar
  ln -sf ${MAPR_HOME}/lib/maprfs-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-java-utils-*.jar
  ln -sf ${MAPR_HOME}/lib/mapr-java-utils-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-tools*.jar
  ln -sf ${MAPR_HOME}/lib/mapr-tools*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-core-asl-1.*.jar
  rm -f  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-jaxrs-1.*.jar
  rm -f  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-mapper-asl-1.*.jar
  rm -f  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-xc-1.*.jar
  ln -sf ${MAPR_HOME}/lib/jackson-core-asl-1.*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf ${MAPR_HOME}/lib/jackson-jaxrs-1.*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf ${MAPR_HOME}/lib/jackson-mapper-asl-1.*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf ${MAPR_HOME}/lib/jackson-xc-1.*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/json-*.jar
  ln -sf ${MAPR_HOME}/lib/json-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/central-logging-*.jar
  ln -sf ${MAPR_HOME}/lib/central-logging-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f  ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/zookeeper-3.*.jar
  ln -sf ${MAPR_HOME}/lib/zookeeper-3.*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf ${MAPR_HOME}/lib/libMapRClient.so ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  if [ -d ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/ ]; then
    ln -sf ${MAPR_HOME}/lib/libMapRClient.so ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/
    ln -sf ${MAPR_HOME}/lib/libjpam.so ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/lib/native
  fi
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprdb-*.jar
  ln -sf ${MAPR_HOME}/lib/maprdb-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-stream*.jar
  ln -sf ${MAPR_HOME}/lib/mapr-stream*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/antlr4-runtime-*.jar
  ln -sf ${MAPR_HOME}/lib/antlr4-runtime-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/ojai-*.jar
  ln -sf ${MAPR_HOME}/lib/ojai-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-security-*.jar
  ln -sf ${MAPR_HOME}/lib/mapr-security-*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jmxagent*.jar
  ln -sf ${MAPR_HOME}/lib/jmxagent*.jar ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/

  rm -f ${MAPR_HOME}/lib/audience-annotations-0.*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/audience-annotations-0.*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/commons-cli-1.*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/commons-cli-1.*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/commons-lang3-3.*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/commons-lang3-3.*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/hadoop-auth-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-auth-__VERSION_3DIGIT__*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/hadoop-yarn-api-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-api-__VERSION_3DIGIT__*.jar ${MAPR_HOME}/lib/
  rm -f  ${MAPR_HOME}/lib/hadoop-yarn-client-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-client-__VERSION_3DIGIT__*.jar ${MAPR_HOME}/lib/
  rm -f  ${MAPR_HOME}/lib/hadoop-yarn-common-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-common-__VERSION_3DIGIT__*.jar ${MAPR_HOME}/lib/

  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/audience-annotations-0.*.jar ${MAPR_HOME}/lib/

  rm -f ${MAPR_HOME}/lib/hadoop-shaded-protobuf_*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-shaded-protobuf_*.jar /opt/mapr/lib
  rm -f ${MAPR_HOME}/lib/hadoop-shaded-guava-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-shaded-guava-*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/woodstox-core-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/woodstox-core-*.jar ${MAPR_HOME}/lib/
  rm -f ${MAPR_HOME}/lib/stax2-api-*.jar
  ln -sf ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/stax2-api-*.jar ${MAPR_HOME}/lib/

  COMMONS_CONFIG_ABSOLUTE=$(find ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib -name "commons-configuration*.jar" -print -quit)
  COMMONS_CONFIG_BASENAME=$(basename ${COMMONS_CONFIG_ABSOLUTE})
  ln -sf ${COMMONS_CONFIG_ABSOLUTE} ${MAPR_HOME}/lib/${COMMONS_CONFIG_BASENAME}

  rm -f ${MAPR_HOME}/lib/hadoop-common-*.jar
  ls ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/hadoop-common-__VERSION_3DIGIT__*.jar | grep -v "tests" | xargs -I {} ln -sf {} ${MAPR_HOME}/lib/.

}

function copyMaprConfFiles() {
  case "$OSTYPE" in
    darwin*)  sed -i '' "s/^yarn_version=.*$/yarn_version=__VERSION_3DIGIT__/" ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version ;;
    *)        sed -i "s/^yarn_version=.*$/yarn_version=__VERSION_3DIGIT__/" ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version ;;
  esac
  if [ -f ${MAPR_HOME}/conf/hadoop_version ]; then
    rm -f ${MAPR_HOME}/conf/hadoop_version
  fi
  cp ${MAPR_HOME}/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version ${MAPR_HOME}/conf/hadoop_version

  DAEMON_CONF=${MAPR_HOME}/conf/daemon.conf
  if [ -f "$DAEMON_CONF" ]; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
    if [ ! -z "$MAPR_USER" ]; then
      chown ${MAPR_USER}:${MAPR_GROUP} ${MAPR_HOME}/conf/hadoop_version
    else
      chown mapr:mapr ${MAPR_HOME}/conf/hadoop_version
    fi
  fi
}

createSymlinks
copyMaprConfFiles

