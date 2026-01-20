#!/bin/bash

GIT_COMMIT=$(git log -1 --pretty=format:"%H")
INSTALLATION_PREFIX=${INSTALLATION_PREFIX:-"/opt/mapr"}
PKG_NAME=${PKG_NAME:-"hadoop"}
PKG_VERSION=${PKG_VERSION:-"3.4.1.300"}
PKG_3DIGIT_VERSION=$(echo "$PKG_VERSION" | cut -d '.' -f 1-3)
TIMESTAMP=${TIMESTAMP:-$(sh -c 'date "+%Y%m%d%H%M"')}
PKG_INSTALL_ROOT=${PKG_INSTALL_ROOT:-"${INSTALLATION_PREFIX}/${PKG_NAME}/${PKG_NAME}-${PKG_3DIGIT_VERSION}"}
DIST_DIR=${DIST_DIR:-"devops/dist"}
ARTIFACTS_VERSION=${ARTIFACTS_VERSION:-"3.4.1.400-dep-1010-SNAPSHOT"}
HADOOP_DEST_NAME=${HADOOP_DEST_NAME:-"hadoop-${ARTIFACTS_VERSION}"}
HADOOP_IMAGE_NAME=${HADOOP_IMAGE_NAME:-"${HADOOP_DEST_NAME}.tar.gz"}
HADOOP_IMAGE_DIST=${HADOOP_IMAGE_DIST:-"hadoop-dist/target"}
# rpmbuild does not work properly when relate path specified here
BUILD_ROOT=${BUILD_ROOT:-"$(pwd)/devops/buildroot"}
