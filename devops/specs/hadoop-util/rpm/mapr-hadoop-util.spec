%undefine __check_files
%global _build_id_links none

summary:     Apache Hadoop util distribution included in HPE DataFabric Software Ecosystem Pack
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-hadoop-util
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   x86_64
obsoletes:   mapr-hadoop-core < 2.7.4, mapr-httpfs < 1.2.0
AutoReqProv: no


%description
Apache Hadoop util distribution included in HPE DataFabric Software Ecosystem Pack
Commit: __GIT_COMMIT__
Branch: __RELEASE_BRANCH__

%clean
echo "NOOP"


%files
__PREFIX__

%pre
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
MY_HD_VERSION="__VERSION_3DIGIT__"
MAPR_HOME=$PREFIX_INSTALL
MY_HD_HOME="__INSTALL_3DIGIT__"
MY_HD_BASE="$( dirname $MY_HD_HOME )"
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

function before_upgrade() {
    #
    # Backup of old configuration files
    #
    echo "Saving old config files"
    MY_OLD_VERSION=$1
    MY_OLD_HD_RVER="$( echo $MY_OLD_VERSION | cut -d'.' -f1-3 )"
    MY_OLD_HD_HOME="$( dirname $MY_HD_HOME )/hadoop-$MY_OLD_HD_RVER"

    mkdir -p $PREFIX_INSTALL/hadoop/hadoop-$MY_OLD_VERSION/etc/hadoop
    cp -r $MY_OLD_HD_HOME/etc/hadoop/* $PREFIX_INSTALL/hadoop/hadoop-${MY_OLD_VERSION}/etc/hadoop

    DAEMON_CONF=$PREFIX_INSTALL/conf/daemon.conf
    if [ -f "$DAEMON_CONF" ]; then
        MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
        MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
        if [ ! -z "$MAPR_USER" ]; then
            chown -R ${MAPR_USER}:${MAPR_GROUP} $PREFIX_INSTALL/hadoop/hadoop-${MY_OLD_VERSION}
        fi
    fi
}

SSL_SERVER="$MY_HD_HOME/etc/hadoop/ssl-server.xml"
SSL_CLIENT="$MY_HD_HOME/etc/hadoop/ssl-client.xml"

if [ -f "$SSL_CLIENT" ] && [ ! -f "$SSL_CLIENT".bak ]; then
  storePass=$(awk '/ssl.client.truststore.password/{getline; print}' "$SSL_CLIENT" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" != "mapr123" ]; then
    cp "$SSL_CLIENT" "$SSL_CLIENT".bak
  fi
fi

if [ -f "$SSL_SERVER" ] && [ ! -f "$SSL_SERVER".bak ]; then
  storePass=$(awk '/ssl.server.keystore.password/{getline; print}' "$SSL_SERVER" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" != "mapr123" ]; then
    cp "$SSL_SERVER" "$SSL_SERVER".bak
  fi
fi

if [ $1 -eq 1 ]; then
    if [ $(rpm -qa | grep "mapr-hadoop-core" | wc -l) -eq 1 ]; then
        echo "Saving old config files"
        MY_OLD_TIMESTAMP=$(rpm -qi mapr-hadoop-core | awk -F': ' '/Version/ {print $2}')
        before_upgrade $MY_OLD_TIMESTAMP
    elif [ -d /opt/mapr/hadoop/hadoop-2.7.0 ]; then
        #copy configuration files at the client node for 6.1 release and below
        MY_OLD_HD_VERSION="2.7.0.old"
        before_upgrade $MY_OLD_HD_VERSION
    fi
fi

if [ $1 -eq 2 ]; then
    MY_OLD_TIMESTAMP=$(rpm -qi mapr-hadoop-util | awk -F': ' '/Version/ {print $2}')
    before_upgrade $MY_OLD_TIMESTAMP

    # "configure.sh -R" needs to know from which old hadoop version (directory) to upgrade
    echo $PREFIX_INSTALL/hadoop/hadoop-$MY_OLD_TIMESTAMP > $PREFIX_INSTALL/hadoop/prior_hadoop_dir

    #
    # Clean up any broken symlinks
    #
    for brokenFile in $(find $PREFIX_INSTALL -type l -exec sh -c "file -b {} | grep -q ^broken" \; -print)
    do
        rm ${brokenFile}
    done
fi


%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
#Changed base home directory based on the installation path
find $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/ -type f -exec \
    sed -i "s|__PREFIX_INSTALL__|$PREFIX_INSTALL|g" {} \;
sed -i "s|__PREFIX_INSTALL__|$PREFIX_INSTALL|g" "$PREFIX_INSTALL/roles/hadoop-util"


touch $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/.not_configured_yet
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/hadoop /usr/bin/hadoop
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/libexec/mapred-config.sh /usr/bin/mapred-config.sh
MY_HD_HOME="__INSTALL_3DIGIT__"
SSL_SERVER="$MY_HD_HOME/etc/hadoop/ssl-server.xml"
SSL_CLIENT="$MY_HD_HOME/etc/hadoop/ssl-client.xml"

if [ -f "$SSL_CLIENT".bak ]; then
  storePass=$(awk '/ssl.client.truststore.password/{getline; print}' "$SSL_CLIENT".bak | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  sed -i -e "s/^\([[:blank:]]*\)<value>mapr123<\/value>.*$/\1<value>$storePass<\/value>/g" "$SSL_CLIENT"

  #Verify if above command worked
  storePassDest=$(awk '/ssl.client.truststore.password/{getline; print}' "$SSL_CLIENT" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" = "$storePassDest" ]; then
    rm -f "$SSL_CLIENT".bak
  fi
fi

if [ -f "$SSL_SERVER".bak ]; then
  storePass=$(awk '/ssl.server.keystore.password/{getline; print}' "$SSL_SERVER".bak | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  sed -i -e "s/^\([[:blank:]]*\)<value>mapr123<\/value>.*$/\1<value>$storePass<\/value>/g" "$SSL_SERVER"

  #Verify if above command worked
  storePassDest=$(awk '/ssl.server.keystore.password/{getline; print}' "$SSL_SERVER" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" = "$storePassDest" ]; then
    rm -f "$SSL_SERVER".bak
  fi
fi

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
MY_HD_HOME="__INSTALL_3DIGIT__"
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
SSL_SERVER="$MY_HD_HOME/etc/hadoop/ssl-server.xml"
SSL_CLIENT="$MY_HD_HOME/etc/hadoop/ssl-client.xml"

if [ -f "$SSL_CLIENT" ] && [ ! -f "$SSL_CLIENT".bak ]; then
  storePass=$(awk '/ssl.client.truststore.password/{getline; print}' "$SSL_CLIENT" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" != "mapr123" ]; then
    cp "$SSL_CLIENT" "$SSL_CLIENT".bak
  fi
fi

if [ -f "$SSL_SERVER" ] && [ ! -f "$SSL_SERVER".bak ]; then
  storePass=$(awk '/ssl.server.keystore.password/{getline; print}' "$SSL_SERVER" | sed 's/\s*<value>\(.*\)<\/value>/\1/')
  if [ "$storePass" != "mapr123" ]; then
    cp "$SSL_SERVER" "$SSL_SERVER".bak
  fi
fi

if [ "$1" = "0" ]; then
    rm -f  /usr/bin/hadoop
    rm -r  /usr/bin/mapred-config.sh

    if [ -d $PREFIX_INSTALL ]; then
        rm -f $PREFIX_INSTALL/conf/ssl-client.xml
        rm -f $PREFIX_INSTALL/conf/ssl-server.xml

        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/slf4j*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-hbase*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mysql-connector-jave*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/libMapRClient.so
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/libMapRClient.so
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/lib/native/libjpam.so
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprfs-*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprutil-*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/json-*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/libprotodefs-*.jar
        rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/central-logging-*.jar

        rm -f  $PREFIX_INSTALL/lib/commons-io-*.jar
        rm -f  $PREFIX_INSTALL/lib/commons-configuration*.jar
        rm -f  $PREFIX_INSTALL/lib/hadoop-auth-__VERSION_3DIGIT__.jar
        rm -f  $PREFIX_INSTALL/lib/hadoop-common-__VERSION_3DIGIT__.jar
        rm -f  $PREFIX_INSTALL/lib/hadoop-yarn-api-__VERSION_3DIGIT__.jar
        rm -f  $PREFIX_INSTALL/lib/htrace-*.jar

        rm -rf $PREFIX_INSTALL/hadoop/OLD_HADOOP_VERSIONS

        rm -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/maprdb-*.jar
        rm -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/mapr-stream*.jar
        rm -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/antlr4-runtime-*.jar
        rm -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/ojai-*.jar
    fi
fi



%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
#
# Clean up any broken symlinks
#
for brokenFile in $(find $PREFIX_INSTALL -type l -exec sh -c "file -b {} | grep -q ^broken" \; -print)
do
    rm ${brokenFile}
done


%posttrans
# $1 -eq 0 install
# $1 -eq 0 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/hadoop /usr/bin/hadoop
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/libexec/mapred-config.sh /usr/bin/mapred-config.sh

#
# Clean up any broken symlinks
#
for brokenFile in $(find $PREFIX_INSTALL -type l -exec sh -c "file -b {} | grep -q ^broken" \; -print)
do
    rm ${brokenFile}
done

#
# during upgrade from 2.7.0 hadoop-core is removing the hadoopversion file
#
if [ ! -f $PREFIX_INSTALL/hadoop/hadoopversion ]; then
touch $PREFIX_INSTALL/hadoop/hadoopversion
echo __VERSION_3DIGIT__ > $PREFIX_INSTALL/hadoop/hadoopversion
fi

#
# clean up any old hadoop directories
#
if [ -n "$(find $PREFIX_INSTALL/hadoop -name 'hadoop-[2-9].[0-9].[0-9].*' | head -1)" ] && [ -f $PREFIX_INSTALL/hadoop/hadoopversion ]; then
    CURRENT_VERSION=$(cat $PREFIX_INSTALL/hadoop/hadoopversion)
    for DIR in $(find $PREFIX_INSTALL/hadoop/ -maxdepth 1 -type d -name "hadoop-[2-9].[0-9].[0-9]" | grep -v "$CURRENT_VERSION")
    do
        BASENAME=$(basename $DIR)
        rm -rf $PREFIX_INSTALL/hadoop/$BASENAME
    done
fi
