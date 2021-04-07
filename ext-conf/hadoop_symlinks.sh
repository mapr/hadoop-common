#!/bin/bash

function createSymlinks() {
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/ssl-client.xml __PREFIX__/conf/ssl-client.xml
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/ssl-server.xml __PREFIX__/conf/ssl-server.xml

  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/failureaccess-*
  ln -sf __PREFIX__/lib/failureaccess-* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/guava-*
  ln -sf __PREFIX__/lib/guava-* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/slf4j-api*
  ln -sf __PREFIX__/lib/slf4j-api* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-hbase-*
  ln -sf __PREFIX__/lib/mapr-hbase-* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mysql-connector-java-*.jar
  ln -sf __PREFIX__/lib/mysql-connector-java-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprfs-*.jar
  ln -sf __PREFIX__/lib/maprfs-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-java-utils-*.jar
  ln -sf __PREFIX__/lib/mapr-java-utils-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-tools*.jar
  ln -sf __PREFIX__/lib/mapr-tools*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-core-asl-1.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-jaxrs-1.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-mapper-asl-1.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-xc-1.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-annotations-2.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-core-2.*.jar
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jackson-databind-2.*.jar
  ln -sf __PREFIX__/lib/jackson-annotations-2.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-core-2.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-core-asl-1.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-databind-2.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-jaxrs-1.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-mapper-asl-1.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/jackson-xc-1.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/json-*.jar
  ln -sf __PREFIX__/lib/json-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/central-logging-*.jar
  ln -sf __PREFIX__/lib/central-logging-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/zookeeper-3.*.jar
  ln -sf __PREFIX__/lib/zookeeper-3.*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/lib/libMapRClient.so __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  if [ -d __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/ ]; then
    ln -sf __PREFIX__/lib/libMapRClient.so __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/
    ln -sf __PREFIX__/lib/libjpam.so __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/lib/native
  fi
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprdb-*.jar
  ln -sf __PREFIX__/lib/maprdb-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-stream*.jar
  ln -sf __PREFIX__/lib/mapr-stream*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/antlr4-runtime-*.jar
  ln -sf __PREFIX__/lib/antlr4-runtime-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/ojai-*.jar
  ln -sf __PREFIX__/lib/ojai-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-security-*.jar
  ln -sf __PREFIX__/lib/mapr-security-*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/jmxagent*.jar
  ln -sf __PREFIX__/lib/jmxagent*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/

  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/audience-annotations-0.*.jar __PREFIX__/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/commons-cli-1.*.jar __PREFIX__/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/htrace-*.jar  __PREFIX__/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-auth-__VERSION_3DIGIT__*.jar __PREFIX__/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-api-__VERSION_3DIGIT__*.jar __PREFIX__/lib/

  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/audience-annotations-0.*.jar __PREFIX__/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/activation-1.*.jar __PREFIX__/lib/

  COMMONS_CONFIG_ABSOLUTE=$(find __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib -name "commons-configuration*.jar" -print -quit)
  COMMONS_CONFIG_BASENAME=$(basename ${COMMONS_CONFIG_ABSOLUTE})
  ln -sf ${COMMONS_CONFIG_ABSOLUTE} __PREFIX__/lib/${COMMONS_CONFIG_BASENAME}

  ls __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/hadoop-common-__VERSION_3DIGIT__*.jar | grep -v "tests" | xargs -I {} ln -sf {} __PREFIX__/lib/.
  rm __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/slf4j-log4j12-* > /dev/null 2>&1
  ln -sf __PREFIX__/lib/slf4j-log4j12-*  __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/

}

function copyMaprConfFiles() {
  case "$OSTYPE" in
    darwin*)  sed -i '' "s/^yarn_version=.*$/yarn_version=__VERSION_3DIGIT__/" __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version ;;
    *)        sed -i "s/^yarn_version=.*$/yarn_version=__VERSION_3DIGIT__/" __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version ;;
  esac
  if [ -f __PREFIX__/conf/hadoop_version ]; then
    rm -f __PREFIX__/conf/hadoop_version
  fi
  cp __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/hadoop_version __PREFIX__/conf/hadoop_version

  DAEMON_CONF=__PREFIX__/conf/daemon.conf
  if [ -f "$DAEMON_CONF" ]; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
    if [ ! -z "$MAPR_USER" ]; then
      chown ${MAPR_USER}:${MAPR_GROUP} __PREFIX__/conf/hadoop_version
    else
      chown mapr:mapr __PREFIX__/conf/hadoop_version
    fi
  fi
}

createSymlinks
copyMaprConfFiles

