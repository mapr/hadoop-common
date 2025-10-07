%undefine __check_files

summary:     Apache Hadoop HttpFS distribution included in HPE DataFabric Software Ecosystem Pack
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-httpfs
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-client, mapr-hadoop-client >= __RELEASE_VERSION__
AutoReqProv: no

%description
Apache Hadoop HttpFS distribution included in HPE DataFabric Software Ecosystem Pack
Commit: __GIT_COMMIT__
Branch: __RELEASE_BRANCH__

%files
__PREFIX__


%clean
echo "NOOP"

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "0" ]; then
    export daemon_user=$(awk -F = '$1 == "mapr.daemon.user" { print $2 }' __PREFIX__/conf/daemon.conf)
    sudo su -c "__PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/bin/hdfs --daemon stop httpfs" -s /bin/sh $daemon_user
    rm -f __PREFIX__/conf/conf.d/warden.httpfs.conf
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
