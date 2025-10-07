%undefine __check_files

summary:     Apache Hadoop core distribution included in HPE DataFabric Software Ecosystem Pack
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-hadoop-core
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   x86_64
requires:    mapr-hadoop-client >= __RELEASE_VERSION__
AutoReqProv: no
%global      PREFIX_INSTALL $RPM_INSTALL_PREFIX__PREFIX__



%description
Apache Hadoop core distribution included in HPE DataFabric Software Ecosystem Pack
Commit: __GIT_COMMIT__
Branch: __RELEASE_BRANCH__

%clean
echo "NOOP"


%files
__PREFIX__

%pre

MY_HD_VERSION="__VERSION_3DIGIT__"
MAPR_HOME=%{PREFIX_INSTALL}
MY_HD_HOME="__INSTALL_3DIGIT__"
MY_HD_BASE="$( dirname $MY_HD_HOME )"

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
#Changed base home directory based on the installation path
find %{PREFIX_INSTALL}/hadoop/hadoop-__VERSION_3DIGIT__/ -type f -exec \
    sed -i "s|__PREFIX_INSTALL__|%{PREFIX_INSTALL}|g" {} \;


%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


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



