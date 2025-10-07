%undefine __check_files

summary:     Apache Hadoop Resourcemanager distribution included in HPE DataFabric Software Ecosystem Pack
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-resourcemanager
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   x86_64
requires:    mapr-hadoop-core >= __RELEASE_VERSION__
AutoReqProv: no


%description
Apache Hadoop Resourcemanager distribution included in HPE DataFabric Software Ecosystem Pack
Commit: __GIT_COMMIT__
Branch: __RELEASE_BRANCH__


%clean
echo "NOOP"


%files
__PREFIX__/

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
    sudo su -c "__INSTALL_3DIGIT__/bin/yarn --daemon stop resourcemanager" -s /bin/sh $daemon_user
    rm -f __PREFIX__/conf/conf.d/warden.resourcemanager.conf
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




