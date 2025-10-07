%undefine __check_files

summary:     Apache Hadoop client distribution included in HPE DataFabric Software Ecosystem Pack
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-hadoop-client
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   x86_64
requires:    mapr-client, mapr-hadoop-util >= __RELEASE_VERSION__
AutoReqProv: no



%description
Apache Hadoop client distribution included in HPE DataFabric Software Ecosystem Pack
Commit: __GIT_COMMIT__
Branch: __RELEASE_BRANCH__


%clean
echo "NOOP"


%files
__PREFIX__

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
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
sed -i "s|__PREFIX_INSTALL__|$PREFIX_INSTALL|g" "$PREFIX_INSTALL/roles/hadoop-client"

touch $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/.not_configured_executor
touch $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/.client

ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/yarn /usr/bin/yarn
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/mapred /usr/bin/mapred
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/hadoop_version_util.sh $PREFIX_INSTALL/bin/hadoop_version_util.sh

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$RPM_INSTALL_PREFIX" != "/" ]; then 
    PREFIX_INSTALL=$RPM_INSTALL_PREFIX__PREFIX__
else
    PREFIX_INSTALL=__PREFIX__
fi
if [ "$1" = "0" ]; then
    rm -f  /usr/bin/yarn
    rm -f  /usr/bin/mapred
    rm -f $PREFIX_INSTALL/bin/hadoop_version_util.sh

    rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/yarn-site.xml

    rm -f  $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/container-executor.cfg.bak

    rm -f  $PREFIX_INSTALL/lib/hadoop-yarn-client-__VERSION_3DIGIT__.jar
    rm -f  $PREFIX_INSTALL/lib/hadoop-yarn-common-__VERSION_3DIGIT__.jar

#
# If doing an upgrade, ...
#
else
    #
    # preserve $PREFIX_INSTALL/hadoop/hadoop-2.X.X/etc/hadoop/container-executor.cfg
    #
    if [ -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/container-executor.cfg ]; then
        cp -f $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/container-executor.cfg $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/etc/hadoop/container-executor.cfg.bak
    fi
fi



%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


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

ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/yarn /usr/bin/yarn
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/mapred /usr/bin/mapred
ln -sf $PREFIX_INSTALL/hadoop/hadoop-__VERSION_3DIGIT__/bin/hadoop_version_util.sh $PREFIX_INSTALL/bin/hadoop_version_util.sh
